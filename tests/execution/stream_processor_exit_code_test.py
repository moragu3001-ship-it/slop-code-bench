"""EXP-001 R2 RED: Windows streaming exit-code race.

Reproduces BENCHMARK_DEFECT WINDOWS_STREAM_EXIT_CODE_RACE:
pipe EOF arrives before the child process is reaped, so an immediate
single poll_fn() returns None even though the process later exits with
the true code. The pre-fix implementation converts that transient None
directly into -1.

These tests must FAIL before the repair and PASS after it.
Preserve this file's RED receipt; do not overwrite after GREEN.
"""

from __future__ import annotations

from slop_code.execution.stream_processor import process_stream


def _run_to_result(stream_factory, timeout, poll_fn, wait_fn=None):
    """Drive process_stream to completion and capture its RuntimeResult."""
    kwargs = {}
    if wait_fn is not None:
        kwargs["wait_fn"] = wait_fn
    gen = process_stream(stream_factory(), timeout, poll_fn, **kwargs)
    try:
        while True:
            next(gen)
    except StopIteration as stop:
        return stop.value
    raise AssertionError("process_stream generator did not return a result")


def _eof_before_reap_poll(calls_before_exit, exit_code):
    """poll_fn that returns None until the process is 'reaped'.

    Simulates the Windows race: at pipe-EOF time poll() is still None;
    the true exit only becomes visible after `calls_before_exit` polls.
    """
    state = {"calls": 0}

    def poll_fn():
        state["calls"] += 1
        if state["calls"] <= calls_before_exit:
            return None
        return exit_code

    return poll_fn


def test_eof_race_exit_0_is_not_minus_one():
    stream = lambda: iter([("hello\n", b"")])  # noqa: E731
    result = _run_to_result(stream, timeout=30, poll_fn=_eof_before_reap_poll(5, 0))
    assert result.exit_code == 0, f"EOF race turned exit 0 into {result.exit_code}"
    assert "hello" in result.stdout


def test_eof_race_exit_5_is_not_minus_one():
    stream = lambda: iter([("out\n", b"")])  # noqa: E731
    result = _run_to_result(stream, timeout=30, poll_fn=_eof_before_reap_poll(5, 5))
    assert result.exit_code == 5, f"EOF race turned exit 5 into {result.exit_code}"


def test_eof_race_exit_42_is_not_minus_one():
    stream = lambda: iter([])  # noqa: E731
    result = _run_to_result(stream, timeout=30, poll_fn=_eof_before_reap_poll(5, 42))
    assert result.exit_code == 42, f"EOF race turned exit 42 into {result.exit_code}"


def test_eof_race_with_wait_fn_reaps_true_exit():
    """Deterministic reap primitive must become authoritative after EOF."""
    stream = lambda: iter([("data\n", b"")])  # noqa: E731
    poll_fn = _eof_before_reap_poll(10**9, 42)  # poll never reaps on its own

    def wait_fn(remaining):
        assert remaining is not None and remaining > 0
        return 42

    result = _run_to_result(stream, timeout=30, poll_fn=poll_fn, wait_fn=wait_fn)
    assert result.exit_code == 42
