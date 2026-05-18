"""Unit tests for PartitionWatermark — pure logic, no I/O."""

from hookrelay.worker.delivery import PartitionWatermark


class TestPartitionWatermark:
    def test_start_adds_offset_to_in_flight(self) -> None:
        wm = PartitionWatermark()
        wm.start(5)
        assert 5 in wm.in_flight

    def test_start_updates_last_seen(self) -> None:
        wm = PartitionWatermark()
        wm.start(3)
        wm.start(7)
        wm.start(5)
        assert wm.last_seen == 7

    def test_done_single_offset_returns_it(self) -> None:
        wm = PartitionWatermark()
        wm.start(10)
        result = wm.done(10)
        assert result == 10
        assert wm.in_flight == set()

    def test_done_higher_offset_first_advances_to_gap_minus_one(self) -> None:
        # Two in-flight offsets. Completing the higher one commits up to min(pending)-1.
        wm = PartitionWatermark()
        wm.start(1)
        wm.start(2)
        # After done(2): in_flight={1}, candidate = min({1})-1 = 0
        result = wm.done(2)
        assert result == 0

    def test_done_returns_none_when_watermark_already_committed(self) -> None:
        wm = PartitionWatermark()
        wm.start(5)
        first = wm.done(5)
        assert first == 5

        # Starting and completing 6 advances to 6.
        wm.start(6)
        second = wm.done(6)
        assert second == 6

        # Completing the same offset again (idempotent discard) — returns None
        # since candidate (6) == last_committed (6).
        wm.start(6)
        wm.in_flight.discard(6)  # simulate double-done
        third = wm.done(6)
        assert third is None

    def test_done_out_of_order_commits_only_when_gap_closes(self) -> None:
        wm = PartitionWatermark()
        wm.start(1)
        wm.start(2)
        wm.start(3)

        # Completing 3 first: in_flight={1,2}, candidate = min(1,2)-1 = 0
        r = wm.done(3)
        assert r == 0

        # Completing 2: in_flight={1}, candidate = min(1)-1 = 0, same as last_committed
        r = wm.done(2)
        assert r is None

        # Completing 1: in_flight={}, candidate = last_seen (3)
        r = wm.done(1)
        assert r == 3

    def test_done_all_in_order(self) -> None:
        wm = PartitionWatermark()
        for i in range(5):
            wm.start(i)

        results = [wm.done(i) for i in range(5)]
        # Each completion with all previous done → candidate advances by 1 each time
        # but only when it exceeds last_committed.
        # After done(0): in_flight={1,2,3,4}, candidate=0, committed=0 → returns 0
        # After done(1): in_flight={2,3,4}, candidate=1, committed=1 → returns 1
        # ...
        assert all(r is not None for r in results)
        assert results[-1] == 4
