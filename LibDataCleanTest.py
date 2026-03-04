import pandas as pd
import unittest

from Library_Data_Cleaner import calculate_days_between_dates

class TestLoanPeriod(unittest.TestCase):

    def test_calculate_days_between_dates(self):

        df = pd.DataFrame({
            "Book checkout": ["01/03/2026","10/03/2026"],
            "Book Returned": ["05/03/2026", "12/03/2026"]
        })

    datecalc = calculate_days_between_dates(df, "Book checkout", "Book Returned", "LoanPeriodDays")
    
    self.assertEqual(datecalc.calculate_days_between_dates,4, "wrong!")


if __name__ == "__nain__":
    unittest.main()

