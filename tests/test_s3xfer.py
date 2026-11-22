import io
import os
import zipfile

import boto3
import pytest
from moto import mock_aws

from s3xfer import BudgetTooSmall, S3RangeStream, parse_size, plan_transfer, s3_copy
from s3xfer.plan import COPY_FACTOR, MAX_PARTS, MiB, GiB

BUCKET_A, BUCKET_B = "src-bucket", "dst-bucket"


@pytest.fixture
def s3():
    with mock_aws():
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
        c = boto3.client("s3", region_name="us-east-1")
        c.create_bucket(Bucket=BUCKET_A)
        c.create_bucket(Bucket=BUCKET_B)
        yield c


def put(s3, key, data, bucket=BUCKET_A):
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    return data


# ---------------- planner -----------------------------------------------------

def test_parse_size():
    assert parse_size("512MB") == 512 * 1000**2
    assert parse_size("1GiB") == GiB
    assert parse_size(42) == 42
    with pytest.raises(ValueError):
        parse_size("lots")


@pytest.mark.parametrize("size", [0, 1, 10 * MiB, 10 * GiB, 1024 * GiB])
@pytest.mark.parametrize("budget", ["64MiB", "512MiB", "2GiB"])
def test_plan_respects_budget_and_s3_limits(size, budget):
    min_chunk = max(5 * MiB, -(-size // MAX_PARTS))
    if min_chunk * COPY_FACTOR > parse_size(budget):
        with pytest.raises(BudgetTooSmall):
            plan_transfer(size, budget, concurrency=16)
        return
    p = plan_transfer(size, budget, concurrency=16)
    assert p.peak_memory <= parse_size(budget)
    assert p.num_parts <= MAX_PARTS
    assert p.chunk_size >= 5 * MiB
    assert p.num_parts * p.chunk_size >= size


def test_file_size_does_not_change_memory():
    small = plan_transfer(1 * GiB, "512MiB", 8)
    huge = plan_transfer(40 * GiB, "512MiB", 8)
    assert small.peak_memory == huge.peak_memory


@pytest.mark.parametrize("concurrency", [1, 2, 4, 8, 16, 32, 64])
def test_more_concurrency_shrinks_chunks_not_budget(concurrency):
    p = plan_transfer(10 * GiB, "256MiB", concurrency)
    assert p.peak_memory <= 256 * MiB
    assert p.chunk_size % MiB == 0


def test_concurrency_is_honoured_by_shrinking_chunks():
    assert plan_transfer(GiB, "256MiB", 8).chunk_size == 16 * MiB
    p = plan_transfer(GiB, "256MiB", 16)
    assert (p.chunk_size, p.concurrency) == (8 * MiB, 16)


def test_chunks_stop_at_s3_minimum_then_streams_drop():
    p = plan_transfer(GiB, "128MiB", 16)
    assert p.chunk_size == 5 * MiB
    assert p.concurrency == 12  # 128 MiB // (5 MiB x 2)


def test_big_budget_does_not_make_huge_chunks():
    p = plan_transfer(GiB, "8GiB", 1)
    assert p.chunk_size == 16 * MiB and p.peak_memory == 32 * MiB


def test_max_chunk_lets_chunks_fill_the_budget_with_warning():
    from s3xfer import LargeChunkWarning
    with pytest.warns(LargeChunkWarning, match="128 MiB chunks"):
        p = plan_transfer(10 * GiB, "2GiB", 8, max_chunk="1GiB")
    assert (p.chunk_size, p.concurrency) == (128 * MiB, 8)
    assert p.peak_memory == 2 * GiB


def test_default_and_forced_big_chunks_do_not_warn(recwarn):
    plan_transfer(10 * GiB, "2GiB", 8)                 # default ceiling: 16 MiB
    plan_transfer(1024 * GiB, "4GiB", 8)               # 10,000-part limit forces 105 MiB
    plan_transfer(20 * MiB, "2GiB", 1, max_chunk="1GiB")  # one part: nothing to retry in pieces
    assert not recwarn.list


def test_chunk_never_bigger_than_file():
    p = plan_transfer(20 * MiB, "2GiB", 1, max_chunk="1GiB")
    assert p.chunk_size == 20 * MiB and p.num_parts == 1


def test_max_chunk_validated():
    with pytest.raises(ValueError):
        plan_transfer(GiB, "1GiB", 1, max_chunk="1MiB")


def test_cli_max_chunk_warns(s3, capsys):
    put(s3, "big", os.urandom(40 * MiB))
    assert main(["cp", f"s3://{BUCKET_A}/big", f"s3://{BUCKET_B}/big",
                 "--memory", "80MiB", "--concurrency", "2", "--max-chunk", "1GiB"]) == 0
    out = capsys.readouterr()
    assert "warning: using 20 MiB chunks" in out.err
    assert "chunk 20 MiB x 2 streams" in out.out
    assert s3.head_object(Bucket=BUCKET_B, Key="big")["ContentLength"] == 40 * MiB


def test_terabyte_file_needs_bigger_parts():
    p = plan_transfer(1024 * GiB, "2GiB", 8)
    assert p.chunk_size >= (1024 * GiB) / MAX_PARTS


def test_budget_too_small():
    with pytest.raises(BudgetTooSmall):
        plan_transfer(1 * GiB, "1MiB")


# ---------------- S3RangeStream ----------------------------------------------

def test_stream_seek_and_read(s3):
    data = put(s3, "blob", os.urandom(3 * MiB + 123))
    st = S3RangeStream(s3, BUCKET_A, "blob", readahead=256 * 1024)
    assert st.seekable() and st.size == len(data)
    st.seek(1_000_000)
    assert st.read(10) == data[1_000_000:1_000_010]
    st.seek(-5, io.SEEK_END)
    assert st.read() == data[-5:]
    st.seek(0)
    assert st.read() == data
    assert st.read(1) == b""


def test_readahead_avoids_one_request_per_small_read(s3):
    put(s3, "blob", os.urandom(1 * MiB))
    st = S3RangeStream(s3, BUCKET_A, "blob", readahead=512 * 1024)
    for _ in range(1000):
        st.read(100)
    assert st.requests == 1


def test_zipfile_reads_directly_from_s3(s3):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("big.bin", os.urandom(2 * MiB))
        z.writestr("hello.txt", "hi from the end of the archive")
    put(s3, "a.zip", buf.getvalue())
    st = S3RangeStream(s3, BUCKET_A, "a.zip")
    with zipfile.ZipFile(st) as z:  # zip index lives at the end -> needs seek
        assert z.read("hello.txt") == b"hi from the end of the archive"


def test_openpyxl_reads_xlsx_from_s3(s3):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    wb.active.append(["id", "name"])
    wb.active.append([1, "tushar"])
    buf = io.BytesIO()
    wb.save(buf)
    put(s3, "sheet.xlsx", buf.getvalue())
    wb2 = openpyxl.load_workbook(S3RangeStream(s3, BUCKET_A, "sheet.xlsx"), read_only=True)
    rows = list(wb2.active.iter_rows(values_only=True))
    assert rows == [("id", "name"), (1, "tushar")]


# ---------------- s3_copy ----------------------------------------------------

@pytest.mark.parametrize("size", [0, 1024, 5 * MiB, 5 * MiB + 1, 23 * MiB + 7])
def test_copy_roundtrip(s3, size):
    data = put(s3, "src", os.urandom(size))
    res = s3_copy(s3, BUCKET_A, "src", BUCKET_B, "dst", memory_budget="64MiB", concurrency=4)
    out = s3.get_object(Bucket=BUCKET_B, Key="dst")["Body"].read()
    assert out == data
    assert res.parts_uploaded == (1 if size <= 5 * MiB else res.plan.num_parts)


def test_copy_retries_transient_failures(s3):
    data = put(s3, "src", os.urandom(12 * MiB))
    real = s3.upload_part
    calls = {"n": 0}

    def flaky(**kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("blip")
        return real(**kw)

    s3.upload_part = flaky
    res = s3_copy(s3, BUCKET_A, "src", BUCKET_B, "dst", memory_budget="64MiB", backoff=0)
    assert res.retries >= 1
    assert s3.get_object(Bucket=BUCKET_B, Key="dst")["Body"].read() == data


def test_copy_aborts_multipart_on_permanent_failure(s3):
    put(s3, "src", os.urandom(12 * MiB))

    def broken(**kw):
        raise ConnectionError("down")

    s3.upload_part = broken
    with pytest.raises(ConnectionError):
        s3_copy(s3, BUCKET_A, "src", BUCKET_B, "dst", memory_budget="64MiB", backoff=0)
    assert s3.list_multipart_uploads(Bucket=BUCKET_B).get("Uploads", []) == []


def test_in_flight_chunks_never_exceed_plan(s3):
    import threading, time
    put(s3, "src", os.urandom(60 * MiB))
    real_get, real_upload = s3.get_object, s3.upload_part
    live, peak, lock = [0], [0], threading.Lock()

    def get(**kw):
        if "Range" in kw:
            with lock:
                live[0] += 1
                peak[0] = max(peak[0], live[0])
        time.sleep(0.01)
        return real_get(**kw)

    def upload(**kw):
        try:
            return real_upload(**kw)
        finally:
            with lock:
                live[0] -= 1

    s3.get_object, s3.upload_part = get, upload
    res = s3_copy(s3, BUCKET_A, "src", BUCKET_B, "dst", memory_budget="80MiB", concurrency=16)
    # 16 streams won't fit: chunks shrink to S3's 5 MiB minimum -> 80 / (5 x 2) = 8 streams
    assert (res.plan.chunk_size, res.plan.concurrency) == (5 * MiB, 8)
    assert res.plan.peak_memory <= 80 * MiB
    assert 1 < peak[0] <= res.plan.concurrency


# ---------------- progress / sync / CLI ----------------------------------------

from s3xfer import plan_sync, s3_sync  # noqa: E402
from s3xfer.__main__ import main  # noqa: E402
from s3xfer.progress import ProgressBar  # noqa: E402


@pytest.mark.parametrize("size", [0, 100, 12 * MiB])
def test_copy_progress_reports_every_byte(s3, size):
    put(s3, "p.bin", os.urandom(size))
    seen = []
    s3_copy(s3, BUCKET_A, "p.bin", BUCKET_B, "p.bin", memory_budget="64MiB", progress=seen.append)
    assert sum(seen) == size


def test_sync_copies_prefix_and_maps_keys(s3):
    put(s3, "data/a.txt", b"a")
    put(s3, "data/sub/b.txt", b"bb")
    put(s3, "data2/not-me.txt", b"x")   # 'data' must not match 'data2/'
    put(s3, "data/folder/", b"")        # folder placeholder is ignored
    res = s3_sync(s3, BUCKET_A, "data", BUCKET_B, "backup/")
    assert sorted(i.dst_key for i in res.copied) == ["backup/a.txt", "backup/sub/b.txt"]
    assert s3.get_object(Bucket=BUCKET_B, Key="backup/sub/b.txt")["Body"].read() == b"bb"
    assert res.bytes_copied == 3


def test_sync_skips_up_to_date_and_recopies_changed(s3):
    put(s3, "d/a", b"a")
    put(s3, "d/b", b"b")
    s3_sync(s3, BUCKET_A, "d/", BUCKET_B, "d/")
    put(s3, "d/b", b"changed")  # different size
    to_copy, skipped = plan_sync(s3, BUCKET_A, "d/", BUCKET_B, "d/")
    assert [i.src_key for i in to_copy] == ["d/b"]
    assert [i.src_key for i in skipped] == ["d/a"]
    to_copy, _ = plan_sync(s3, BUCKET_A, "d/", BUCKET_B, "d/", force=True)
    assert len(to_copy) == 2


def test_sync_whole_bucket(s3):
    put(s3, "x", b"1")
    put(s3, "y/z", b"2")
    res = s3_sync(s3, BUCKET_A, "", BUCKET_B, "")
    assert sorted(i.dst_key for i in res.copied) == ["x", "y/z"]


def test_progress_bar_draws_on_tty():
    class Tty(io.StringIO):
        def isatty(self):
            return True
    out = Tty()
    bar = ProgressBar(100, 2, stream=out, min_interval=0)
    bar.start_file(0, "a", 50)
    bar.update(50)
    bar.update(50)
    bar.close()
    assert "100%" in out.getvalue() and "\r" in out.getvalue()


def test_progress_bar_plain_lines_when_not_tty():
    out = io.StringIO()
    bar = ProgressBar(10, 2, stream=out)
    bar.start_file(0, "a.txt", 5)
    bar.start_file(1, "b.txt", 5)
    bar.close()
    assert out.getvalue().splitlines() == ["[1/2] a.txt (5 B)", "[2/2] b.txt (5 B)"]


def test_cli_cp_and_sync(s3, capsys):
    put(s3, "in/a", b"hello")
    put(s3, "in/b", b"world!")
    assert main(["cp", f"s3://{BUCKET_A}/in/a", f"s3://{BUCKET_B}/one"]) == 0
    assert s3.get_object(Bucket=BUCKET_B, Key="one")["Body"].read() == b"hello"
    assert main(["sync", f"s3://{BUCKET_A}/in", f"s3://{BUCKET_B}/out"]) == 0
    assert main(["sync", f"s3://{BUCKET_A}/in", f"s3://{BUCKET_B}/out"]) == 0
    err = capsys.readouterr().err
    assert "2 to copy" in err and "0 to copy (0 B), 2 already up to date" in err


@pytest.mark.parametrize("cmd", ["cp", "sync"])
def test_cli_uses_separate_profiles_for_src_and_dst(s3, monkeypatch, cmd):
    """--src-profile builds the reading client, --dst-profile the writing one."""
    from unittest import mock

    put(s3, "in/a", b"hello")
    made = []  # (profile, client)
    real_session = boto3.Session

    def session(profile_name=None, **kw):
        sess = real_session(**kw)  # moto doesn't care about real profiles
        real_client = sess.client

        def client(*a, **k):
            c = mock.Mock(wraps=real_client(*a, **k))
            made.append((profile_name, c))
            return c
        sess.client = client
        return sess

    monkeypatch.setattr(boto3, "Session", session)
    src = f"s3://{BUCKET_A}/in/a" if cmd == "cp" else f"s3://{BUCKET_A}/in"
    dst = f"s3://{BUCKET_B}/out/a" if cmd == "cp" else f"s3://{BUCKET_B}/out"
    assert main([cmd, src, dst, "--src-profile", "reader", "--dst-profile", "writer", "-q"]) == 0

    clients = dict(made)
    assert set(clients) == {"reader", "writer"}
    assert clients["reader"].get_object.called and not clients["reader"].put_object.called
    assert clients["writer"].put_object.called and not clients["writer"].get_object.called
    assert s3.get_object(Bucket=BUCKET_B, Key="out/a")["Body"].read() == b"hello"


def test_cli_without_dst_profile_uses_one_client(s3, monkeypatch):
    put(s3, "k", b"x")
    profiles = []
    real_session = boto3.Session
    monkeypatch.setattr(boto3, "Session",
                        lambda profile_name=None, **kw: profiles.append(profile_name) or real_session(**kw))
    assert main(["cp", f"s3://{BUCKET_A}/k", f"s3://{BUCKET_B}/k", "--src-profile", "only", "-q"]) == 0
    assert profiles == ["only"]


# ---------------- architecture: layering and composition --------------------------

import ast  # noqa: E402
from pathlib import Path  # noqa: E402

from s3xfer import (Copier, NullEvents, RetryPolicy, S3Path, Syncer,  # noqa: E402
                    TransferConfig)

PKG = Path(__file__).resolve().parents[1] / "src" / "s3xfer"

# Each module may only import the ones listed (lower layers). Anything else,
# including an import cycle, fails this test.
ALLOWED = {
    "access": {"paths"},
    "paths": set(),
    "plan": set(),
    "retry": set(),
    "events": {"paths", "plan"},
    "progress": {"paths"},
    "stream": {"access", "paths", "plan"},
    "copy": {"access", "events", "paths", "plan", "retry"},
    "sync": {"copy", "events", "paths", "plan", "retry"},
    "cli": {"copy", "paths", "plan", "progress", "sync"},
    "__main__": {"cli"},
}


def _internal_imports(module: str) -> set[str]:
    tree = ast.parse((PKG / f"{module}.py").read_text())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 1:
            found.add(node.module)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for inner in ast.walk(node):
                assert not isinstance(inner, (ast.Import, ast.ImportFrom)), \
                    f"{module}.{node.name} imports inside a function"
    return found


def test_every_module_is_covered_by_the_layering_rules():
    modules = {p.stem for p in PKG.glob("*.py")} - {"__init__"}
    assert modules == set(ALLOWED)


@pytest.mark.parametrize("module", sorted(ALLOWED))
def test_module_imports_only_lower_layers(module):
    assert _internal_imports(module) <= ALLOWED[module]


def test_import_graph_has_no_cycles():
    graph = {m: _internal_imports(m) for m in ALLOWED}
    done, visiting = set(), set()

    def visit(m, path):
        assert m not in visiting, f"import cycle: {' -> '.join(path + [m])}"
        if m in done:
            return
        visiting.add(m)
        for dep in graph[m]:
            visit(dep, path + [m])
        visiting.discard(m)
        done.add(m)

    for m in graph:
        visit(m, [])


class RecordingEvents(NullEvents):
    def __init__(self):
        self.calls, self.bytes = [], 0

    def start(self, items, skipped):
        self.calls.append(("start", len(items), len(skipped)))

    def file_started(self, index, item):
        self.calls.append(("file", index, item.src_key))

    def advance(self, nbytes):
        self.bytes += nbytes

    def warning(self, message):
        self.calls.append(("warning", message.split(" (")[0]))

    def finished(self):
        self.calls.append(("finished",))


def test_copier_reports_one_run_to_its_events(s3):
    put(s3, "obj", os.urandom(40 * MiB))
    ev = RecordingEvents()
    res = Copier(s3, events=ev, config=TransferConfig("64MiB", 2)).copy(
        S3Path(BUCKET_A, "obj"), S3Path(BUCKET_B, "obj"))
    assert ev.calls == [("start", 1, 0), ("file", 0, "obj"), ("finished",)]
    assert ev.bytes == 40 * MiB
    assert (res.plan.chunk_size, res.plan.concurrency, res.parts_uploaded) == (16 * MiB, 2, 3)


def test_copier_reports_finished_even_when_the_copy_fails(s3):
    put(s3, "obj", os.urandom(12 * MiB))
    ev = RecordingEvents()

    def boom(**kw):
        raise RuntimeError("upload failed")

    s3.upload_part = boom
    with pytest.raises(RuntimeError):
        Copier(s3, events=ev, retry=RetryPolicy(1)).copy(S3Path(BUCKET_A, "obj"), S3Path(BUCKET_B, "obj"))
    assert ev.calls[-1] == ("finished",)


def test_copier_sends_large_chunk_warning_to_events_not_python_warnings(s3, recwarn):
    put(s3, "big", os.urandom(40 * MiB))
    ev = RecordingEvents()
    Copier(s3, events=ev, config=TransferConfig("80MiB", 2, "1GiB")).copy(
        S3Path(BUCKET_A, "big"), S3Path(BUCKET_B, "big"))
    assert ("warning", "using 20 MiB chunks") in ev.calls
    assert not recwarn.list


def test_syncer_reports_one_run_for_many_files(s3):
    put(s3, "p/a", b"a")
    put(s3, "p/b", b"bb")
    ev = RecordingEvents()
    res = Syncer(Copier(s3, events=ev)).sync(S3Path(BUCKET_A, "p"), S3Path(BUCKET_B, "q"))
    assert ev.calls == [("start", 2, 0), ("file", 0, "p/a"), ("file", 1, "p/b"), ("finished",)]
    assert [i.dst for i in res.copied] == [S3Path(BUCKET_B, "q/a"), S3Path(BUCKET_B, "q/b")]


def test_retry_policy_is_injected(s3):
    put(s3, "obj", b"x" * 100)
    calls = []
    real = s3.get_object

    def flaky(**kw):
        calls.append(1)
        if len(calls) < 4:
            raise RuntimeError("transient")
        return real(**kw)

    s3.get_object = flaky
    res = Copier(s3, retry=RetryPolicy(max_attempts=4, backoff=0)).copy(
        S3Path(BUCKET_A, "obj"), S3Path(BUCKET_B, "obj"))
    assert res.retries == 3
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)


def test_s3path_parse():
    assert S3Path.parse("s3://b/k/x") == S3Path("b", "k/x")
    assert S3Path.parse("s3://b", allow_empty_key=True) == S3Path("b", "")
    for bad in ("s3://b", "http://b/k", "b/k"):
        with pytest.raises(ValueError):
            S3Path.parse(bad)


# ---------------- credentials: private and least-privilege -------------------------

import copy as copy_module  # noqa: E402
import pickle  # noqa: E402

from s3xfer.access import DestWriter, SourceReader  # noqa: E402

READ_OPS = {"head_object", "get_object", "get_paginator"}
WRITE_OPS = {"put_object", "create_multipart_upload", "upload_part",
             "complete_multipart_upload", "abort_multipart_upload", "get_paginator"}


class OnlyOps:
    """A client proxy that fails the test on any S3 call outside `allowed`."""

    def __init__(self, client, allowed, side):
        self._client, self._allowed, self._side, self.used = client, allowed, side, set()

    def __getattr__(self, name):
        if name not in self._allowed:
            raise AssertionError(f"{self._side} client used for {name}")
        self.used.add(name)
        return getattr(self._client, name)


@pytest.mark.parametrize("size", [100, 40 * MiB])  # single PUT and multipart
def test_copy_reads_only_with_source_and_writes_only_with_dest(s3, size):
    put(s3, "obj", os.urandom(size))
    src, dst = OnlyOps(s3, READ_OPS, "source"), OnlyOps(s3, WRITE_OPS, "dest")
    Copier(src, dst, config=TransferConfig("64MiB", 2)).copy(S3Path(BUCKET_A, "obj"), S3Path(BUCKET_B, "obj"))
    assert src.used <= {"head_object", "get_object"}
    assert "get_object" not in dst.used and "head_object" not in dst.used


def test_sync_reads_only_with_source_and_writes_only_with_dest(s3):
    put(s3, "p/a", b"a")
    put(s3, "p/big", os.urandom(12 * MiB))
    src, dst = OnlyOps(s3, READ_OPS, "source"), OnlyOps(s3, WRITE_OPS, "dest")
    res = Syncer(Copier(src, dst)).sync(S3Path(BUCKET_A, "p"), S3Path(BUCKET_B, "q"))
    assert len(res.copied) == 2


def test_failed_copy_aborts_with_dest_only(s3):
    put(s3, "obj", os.urandom(12 * MiB))
    src, dst = OnlyOps(s3, READ_OPS, "source"), OnlyOps(s3, WRITE_OPS, "dest")
    s3.upload_part = lambda **kw: (_ for _ in ()).throw(RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        Copier(src, dst, retry=RetryPolicy(1)).copy(S3Path(BUCKET_A, "obj"), S3Path(BUCKET_B, "obj"))
    assert "abort_multipart_upload" in dst.used


def test_wrappers_expose_only_their_side():
    assert not hasattr(SourceReader, "put") and not hasattr(SourceReader, "upload_part")
    assert not hasattr(DestWriter, "read") and not hasattr(DestWriter, "size")


class Secret:
    def __repr__(self):
        return "Secret(AKIAEXAMPLE)"


@pytest.mark.parametrize("make", [
    lambda c: SourceReader(c),
    lambda c: DestWriter(c),
    lambda c: Copier(c),
    lambda c: Syncer(Copier(c)),
])
def test_nothing_exposes_the_client(make):
    obj = make(Secret())
    assert "AKIA" not in repr(obj) and "Secret" not in repr(obj)
    public = [a for a in dir(obj) if not a.startswith("_")]
    for name in public:
        value = getattr(obj, name, None)
        assert not isinstance(value, Secret), f"{type(obj).__name__}.{name} returns the client"
    assert not hasattr(obj, "__dict__"), "slots only: no ad-hoc attributes"
    for leak in (pickle.dumps, copy_module.copy, copy_module.deepcopy):
        with pytest.raises(TypeError):
            leak(obj)


def test_range_stream_keeps_client_private(s3):
    put(s3, "f", b"0123456789")
    st = S3RangeStream(s3, BUCKET_A, "f")
    assert st.read(4) == b"0123"
    assert "client" not in " ".join(a for a in dir(st) if not a.startswith("_"))
    with pytest.raises(TypeError):
        pickle.dumps(st)
