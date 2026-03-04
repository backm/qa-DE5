"""
Library Data Cleaner + SQL Loader (refactored)

Key behaviour:
- Importing this module does NOT run the cleaning pipeline.
- Two classes:
    - Cleaner: reads raw CSVs, cleans, validates, writes cleaned/error files
    - SQLLoad: loads raw/clean/error files into existing SQL tables (no DDL)

This is designed so unit tests can safely do:
    from Library_Data_Cleaner_refactored_classes import calculate_days_between_dates

...without triggering the full cleaner.
"""

from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import pyodbc


# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = Path("Data")

BOOKS_RAW_FILE = DATA_DIR / "03_Library Systembook.csv"
CUSTOMERS_RAW_FILE = DATA_DIR / "03_Library SystemCustomers.csv"

BOOKS_CLEAN_FILE = DATA_DIR / "books_cleaned.csv"
BOOKS_ERRORS_FILE = DATA_DIR / "books_problem_rows.csv"
CUSTOMERS_CLEAN_FILE = DATA_DIR / "customers_cleaned.csv"

# SQL target table names (assumed to ALREADY exist)
SQL_TABLES = {
    "books_raw": "bronze.books_raw",
    "customers_raw": "bronze.customers_raw",
    "books_clean": "silver.books_clean",
    "customers_clean": "silver.customers_clean",
    "books_errors": "audit.books_errors",
}


# -----------------------------
# CLEANER
# -----------------------------
class Cleaner:
    """
    Cleans raw library CSVs and writes cleaned + problem-row outputs.
    """

    def __init__(
        self,
        books_file: Path = BOOKS_RAW_FILE,
        customers_file: Path = CUSTOMERS_RAW_FILE,
        out_books_clean: Path = BOOKS_CLEAN_FILE,
        out_books_errors: Path = BOOKS_ERRORS_FILE,
        out_customers_clean: Path = CUSTOMERS_CLEAN_FILE,
    ) -> None:
        self.books_file = Path(books_file)
        self.customers_file = Path(customers_file)
        self.out_books_clean = Path(out_books_clean)
        self.out_books_errors = Path(out_books_errors)
        self.out_customers_clean = Path(out_customers_clean)

    # ---- helpers (kept simple & explicit for learning) ----
    @staticmethod
    def check_file_exists(file_path: Path) -> None:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"File not found: {file_path}")

    @staticmethod
    def load_csv(file_path: Path) -> pd.DataFrame:
        return pd.read_csv(file_path, dtype=str)

    @staticmethod
    def clean_text_column(df: pd.DataFrame, col_name: str) -> None:
        """
        Mutates df: strips quotes + trims whitespace
        """
        if col_name not in df.columns:
            return
        df[col_name] = (
            df[col_name]
            .astype(str)
            .str.replace('"', "", regex=False)
            .str.strip()
        )

    @staticmethod
    def convert_to_date(df: pd.DataFrame, col_name: str) -> None:
        """
        Mutates df: converts UK date strings to datetime
        """
        if col_name not in df.columns:
            return
        df[col_name] = pd.to_datetime(df[col_name], format="%d/%m/%Y", errors="coerce")

    @staticmethod
    def calculate_days_between_dates(
        df: pd.DataFrame,
        start_col: str,
        end_col: str,
        new_col: str = "LoanPeriodDays",
    ) -> pd.DataFrame:
        """
        Returns a COPY of df with a new column for whole-day difference.
        Assumes start_col/end_col are datetime-like. If they are strings, this may fail
        (use convert_to_date first) — which is useful for demonstrating test failures.
        """
        out = df.copy()
        out[new_col] = (out[end_col] - out[start_col]).dt.days
        # keep as nullable int for clean SQL inserts / missing values
        out[new_col] = out[new_col].astype("Int64")
        return out

    @staticmethod
    def convert_weeks_to_days(df: pd.DataFrame) -> None:
        """
        Mutates df: converts "Allowed Weeks" column into "Allowed Days" if present.
        """
        if "Allowed Weeks" in df.columns and "Allowed Days" not in df.columns:
            df["Allowed Days"] = pd.to_numeric(df["Allowed Weeks"], errors="coerce") * 7

    @staticmethod
    def validate_book_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a df of problematic rows (invalid dates / checkout after return).
        Adds boolean flags to help auditing.
        """
        problems = df.copy()

        problems["Invalid Checkout Date"] = problems["Book checkout"].isna()
        problems["Invalid Return Date"] = problems["Book Returned"].isna()

        # Only compare where both dates exist
        both_present = problems["Book checkout"].notna() & problems["Book Returned"].notna()
        problems["Checkout After Return"] = False
        problems.loc[both_present, "Checkout After Return"] = (
            problems.loc[both_present, "Book checkout"] > problems.loc[both_present, "Book Returned"]
        )

        bad = problems[
            problems["Invalid Checkout Date"]
            | problems["Invalid Return Date"]
            | problems["Checkout After Return"]
        ]

        return bad

    # ---- main cleaning pipeline ----
    def run_cleaning(self) -> None:
        print("Starting Library data cleaning process...")

        # file checks
        self.check_file_exists(self.books_file)
        self.check_file_exists(self.customers_file)
        print("Files found. Loading...")

        # load
        books_df = self.load_csv(self.books_file)
        customers_df = self.load_csv(self.customers_file)
        print("Files loaded successfully.")

        # clean text columns
        self.clean_text_column(books_df, "Book checkout")
        self.clean_text_column(books_df, "Book Returned")
        self.clean_text_column(books_df, "Books")
        print("Quotes removed and titles trimmed...")

        # date conversions
        self.convert_to_date(books_df, "Book checkout")
        self.convert_to_date(books_df, "Book Returned")
        print("Date conversion complete.")

        # calc loan period
        books_df = self.calculate_days_between_dates(
            books_df,
            start_col="Book checkout",
            end_col="Book Returned",
            new_col="LoanPeriodDays",
        )

        # allowed weeks -> days
        self.convert_weeks_to_days(books_df)
        print("Borrow time converted.")

        # validate + split
        bad_rows = self.validate_book_data(books_df)
        print("Number of problematic rows:", len(bad_rows))

        clean_books_df = books_df.drop(bad_rows.index)

        # ensure output dir exists
        self.out_books_clean.parent.mkdir(parents=True, exist_ok=True)

        # write outputs
        clean_books_df.to_csv(self.out_books_clean, index=False)
        bad_rows.to_csv(self.out_books_errors, index=False)
        customers_df.to_csv(self.out_customers_clean, index=False)

        print("Cleaning complete.")
        print("Cleaned files saved.")


# Convenience wrapper for tests that want to import a function directly
def calculate_days_between_dates(df, start_col, end_col, new_col="LoanPeriodDays"):
    return Cleaner.calculate_days_between_dates(df, start_col, end_col, new_col)


# -----------------------------
# SQL LOADER
# -----------------------------
class SQLLoad:
    """
    Loads CSV outputs into existing SQL tables.
    Premise: tables already exist (no DDL in this script).
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        trusted_connection: str = "yes",
    ) -> None:
        self.server = server
        self.database = database
        self.driver = driver
        self.trusted_connection = trusted_connection

    def connect(self):
        conn_str = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection={self.trusted_connection};"
        )
        return pyodbc.connect(conn_str)

    @staticmethod
    def insert_df(conn, table_name: str, df: pd.DataFrame) -> None:
        cols = list(df.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join([f"[{c}]" for c in cols])

        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        cur = conn.cursor()
        cur.fast_executemany = True

        rows = df.astype(object).where(pd.notnull(df), None).values.tolist()
        cur.executemany(sql, rows)
        conn.commit()

    @staticmethod
    def insert_df_raw_strings(conn, table_name: str, df: pd.DataFrame) -> None:
        # Force everything to string/None so NVARCHAR inserts don't hit numeric issues
        df2 = df.copy()
        df2 = df2.where(pd.notnull(df2), None)

        for col in df2.columns:
            df2[col] = df2[col].map(lambda x: None if x is None else str(x))

        cols = list(df2.columns)
        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join([f"[{c}]" for c in cols])

        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        cur = conn.cursor()
        cur.fast_executemany = True
        cur.executemany(sql, df2.values.tolist())
        conn.commit()

    @staticmethod
    def prep_books_for_silver(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # numeric columns → nullable ints
        for col in ["Id", "CustomerID", "AllowedDays", "LoanPeriodDays"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

        # dates → python date objects
        for col in ["CheckoutDate", "ReturnDate"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        df = df.astype(object).where(pd.notnull(df), None)
        return df

    def load_to_sql(
        self,
        books_raw_file: Path = BOOKS_RAW_FILE,
        customers_raw_file: Path = CUSTOMERS_RAW_FILE,
        books_clean_file: Path = BOOKS_CLEAN_FILE,
        customers_clean_file: Path = CUSTOMERS_CLEAN_FILE,
        books_errors_file: Path = BOOKS_ERRORS_FILE,
    ) -> None:
        print("Loading data to SQL Database: Newham Library...")

        # basic file checks
        for p in [books_raw_file, customers_raw_file, books_clean_file, customers_clean_file]:
            if not Path(p).exists():
                raise FileNotFoundError(f"Missing required file for SQL load: {p}")

        conn = self.connect()

        # BRONZE LOAD (raw)
        books_raw = pd.read_csv(books_raw_file, dtype=str)
        books_raw["SourceFile"] = Path(books_raw_file).name
        self.insert_df_raw_strings(conn, SQL_TABLES["books_raw"], books_raw)

        customers_raw = pd.read_csv(customers_raw_file, dtype=str)
        customers_raw["SourceFile"] = Path(customers_raw_file).name
        self.insert_df_raw_strings(conn, SQL_TABLES["customers_raw"], customers_raw)

        # SILVER LOAD (clean)
        books_clean = pd.read_csv(books_clean_file)

        # rename columns to match SQL schema (same as your original approach)
        rename_map = {
            "Books": "BookTitle",
            "Customer ID": "CustomerID",
            "Book checkout": "CheckoutDate",
            "Book Returned": "ReturnDate",
            "Allowed Days": "AllowedDays",
        }
        for src, dst in rename_map.items():
            if src in books_clean.columns:
                books_clean = books_clean.rename(columns={src: dst})

        books_clean = self.prep_books_for_silver(books_clean)
        self.insert_df(conn, SQL_TABLES["books_clean"], books_clean)

        customers_clean = pd.read_csv(customers_clean_file, dtype=str)
        customers_clean["SourceFile"] = Path(customers_clean_file).name
        self.insert_df_raw_strings(conn, SQL_TABLES["customers_clean"], customers_clean)

        # AUDIT LOAD (errors)
        if Path(books_errors_file).exists():
            books_err = pd.read_csv(books_errors_file, dtype=str)

            # align columns to expected audit schema
            if "Books" in books_err.columns:
                books_err = books_err.rename(columns={"Books": "BookTitle"})
            if "Book checkout_raw" in books_err.columns:
                books_err = books_err.rename(columns={"Book checkout_raw": "CheckoutRaw"})
            if "Book Returned_raw" in books_err.columns:
                books_err = books_err.rename(columns={"Book Returned_raw": "ReturnRaw"})
            if "Customer ID" in books_err.columns:
                books_err = books_err.rename(columns={"Customer ID": "CustomerID"})

            audit_books = books_err.reindex(
                columns=[
                    "Id", "BookTitle", "CheckoutRaw", "ReturnRaw", "CustomerID",
                    "Invalid Checkout Date", "Invalid Return Date", "Checkout After Return"
                ],
                fill_value=None
            ).rename(columns={
                "Invalid Checkout Date": "InvalidCheckoutDate",
                "Invalid Return Date": "InvalidReturnDate",
                "Checkout After Return": "CheckoutAfterReturn"
            }).assign(SourceFile=Path(books_raw_file).name)

            # Convert flags to 0/1 for BIT columns
            for c in ["InvalidCheckoutDate", "InvalidReturnDate", "CheckoutAfterReturn"]:
                audit_books[c] = audit_books[c].map(
                    lambda x: 1 if str(x).strip().lower() in ("true", "1", "yes") else 0
                )

            self.insert_df(conn, SQL_TABLES["books_errors"], audit_books)

        conn.close()
        print("SQL tables loaded.")


# -----------------------------
# ENTRY POINT
# -----------------------------
def main():
    # 1) Clean files
    cleaner = Cleaner()
    cleaner.run_cleaning()

    # 2) Load to SQL (optional) — set env vars or hardcode for your environment
    # If you don't want SQL load every time, comment these lines out.
    server = os.getenv("LIB_SQL_SERVER")
    database = os.getenv("LIB_SQL_DATABASE")

    if server and database:
        loader = SQLLoad(server=server, database=database)
        loader.load_to_sql()
    else:
        print("SQL load skipped (set LIB_SQL_SERVER and LIB_SQL_DATABASE to enable).")


if __name__ == "__main__":
    main()
