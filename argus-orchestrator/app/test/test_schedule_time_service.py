
import unittest
from datetime import datetime, timezone

from app.services.schedule_time_service import calculate_next_run


class TestCalculateNextRun(unittest.TestCase):

    RUN_TIMES = ["10:00", "19:00"]

    def test_before_first_run(self):
        now = datetime(
            2026, 10, 13, 4, 15, tzinfo=timezone.utc
        )
        result = calculate_next_run(now, self.RUN_TIMES)

        self.assertEqual(
            result,
            datetime(2026, 10, 13, 4, 30, tzinfo=timezone.utc),
        )

    def test_between_daily_runs(self):
        now = datetime(
            2026, 10, 13, 4, 35, tzinfo=timezone.utc
        )
        result = calculate_next_run(now, self.RUN_TIMES)

        self.assertEqual(
            result,
            datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc),
        )

    def test_after_last_run(self):
        now = datetime(
            2026, 10, 13, 14, 0, tzinfo=timezone.utc
        )
        result = calculate_next_run(now, self.RUN_TIMES)

        self.assertEqual(
            result,
            datetime(2026, 10, 14, 4, 30, tzinfo=timezone.utc),
        )

    def test_exact_run_time_moves_to_next_run(self):
        now = datetime(
            2026, 10, 13, 4, 30, tzinfo=timezone.utc
        )
        result = calculate_next_run(now, self.RUN_TIMES)

        self.assertEqual(
            result,
            datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc),
        )

    def test_empty_run_times_rejected(self):
        with self.assertRaises(ValueError):
            calculate_next_run(
                datetime.now(timezone.utc),
                [],
            )


if __name__ == "__main__":
    unittest.main()
