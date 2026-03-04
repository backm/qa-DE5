import unittest
import pandas as pd

from Library_Data_Cleaner import calculate_days_between_dates


class TestLoanPeriod(unittest.TestCase):

    def test_loan_period_days_column(self):
        df = pd.DataFrame({
            "Book checkout": ["01/03/2026", "10/03/2026", None, "20/03/2026"],
            "Book Returned": ["05/03/2026", "10/03/2026", "12/03/2026", "18/03/2026"],
        })

        df["Book checkout"] = pd.to_datetime(df["Book checkout"], format="%d/%m/%Y", errors="coerce")
        df["Book Returned"] = pd.to_datetime(df["Book Returned"], format="%d/%m/%Y", errors="coerce")

        out = calculate_days_between_dates(df, "Book checkout", "Book Returned", "LoanPeriodDays")

        expected = [4, 0, pd.NA, -2]
        actual = out["LoanPeriodDays"].tolist()

        # `pd.NA` is awkward with direct equality, so handle it:
        self.assertEqual(actual[0], expected[0])
        self.assertEqual(actual[1], expected[1])
        self.assertTrue(pd.isna(actual[2]))
        self.assertEqual(actual[3], expected[3])


if __name__ == "__main__":
    unittest.main()
