"""Tests for compaction planning utilities."""

from __future__ import annotations

from spark.optimization.compact_tables import compute_target_file_count


def test_target_file_count_respects_bounds() -> None:
    count = compute_target_file_count(
        total_size_bytes=500 * 1024 * 1024,
        target_file_size_bytes=128 * 1024 * 1024,
        min_file_count=1,
        max_file_count=3,
    )
    assert count == 3


def test_target_file_count_for_empty_dataset() -> None:
    count = compute_target_file_count(
        total_size_bytes=0,
        target_file_size_bytes=128 * 1024 * 1024,
        min_file_count=1,
        max_file_count=10,
    )
    assert count == 1
