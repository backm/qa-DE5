
#data cleaner intended for use with Library_System data; book and customer csv
#m.backhouse
#2026-03-03

#------------------------------------------------------------------

#CONFIG

import pandas as pd
import os
import sys
import pyodbc
from pathlib import Path

#env requires "pip install pyodbc pandas" for SQL actions

books_file = "Data/03_Library Systembook.csv"
customers_file = "Data/03_Library SystemCustomers.csv"


#for sql import - doesnt fail if there are no errors
REQUIRED = [
    "books_raw",
    "customers_raw",
    "books_clean",
    "customers_clean"
]

OPTIONAL = [
    "books_errors",
    "customers_errors"
]

#-----------------------------------------------------------------

#CLEANER FUNCTIONS


def check_file_exists(file_path):

    if not os.path.exists(file_path):
        print("ERROR: File not found:", file_path)
        sys.exit()


def load_csv(file_path):

    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print("ERROR: Could not read file:", file_path)
        print("Details:", e)
        sys.exit()

def clean_text_column(df, column_name):

    df[column_name] = (
        df[column_name]
        .astype(str)
        .str.replace('"', '', regex=False)
        .str.strip()
    )

def convert_to_date(df, column_name):

    df[column_name] = pd.to_datetime(
        df[column_name],
        format="%d/%m/%Y",
        errors="coerce"
    )

def calculate_days_between_dates(df, start_col, end_col, new_col="LoanPeriodDays"):

    df = df.copy()

    # Ensure datetime (safe if already datetime)
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")

    # Timedelta -> days (float if NaT involved), then to nullable Int64
    df[new_col] = (df[end_col] - df[start_col]).dt.days.astype("Int64")

    return df

def convert_weeks_to_days(df):

    df["Allowed Days"] = (
        df["Days allowed to borrow"]
        .str.replace(" weeks", "", regex=False)
        .str.replace(" week", "", regex=False)
        .astype(float) * 7
    )

def validate_book_data(df):

    # Flag missing or invalid dates
    df["Invalid Checkout Date"] = df["Book checkout"].isna()
    df["Invalid Return Date"] = df["Book Returned"].isna()

    # Flag logical error (checkout after return)
    df["Checkout After Return"] = (
        df["Book checkout"] > df["Book Returned"]
    )

    # Identify problematic rows
    bad_rows = df[
        (df["Invalid Checkout Date"]) |
        (df["Invalid Return Date"]) |
        (df["Checkout After Return"])
    ]

    return bad_rows

#-----------------------------------------------------------------
#SQL CONFIG

SERVER = r"localhost"
DATABASE = "NewhamLibrary"

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

DATA_DIR = Path("Data")

FILES = {
    "books_clean": DATA_DIR / "books_cleaned.csv",
    "books_errors": DATA_DIR / "books_problem_rows.csv",
    "customers_clean": DATA_DIR / "customers_cleaned.csv",
    "customers_errors": DATA_DIR / "customers_problem_rows.csv",
    "books_raw": DATA_DIR / "03_Library Systembook.csv",
    "customers_raw": DATA_DIR / "03_Library SystemCustomers.csv",
}

#-----------------------------------------------------------------
#RUN

print("Starting Library data cleaning process...")

#run file checker
check_file_exists(books_file)
check_file_exists(customers_file)

print("Files found. Loading...")

# run file loader

books_df = load_csv(books_file)
customers_df = load_csv(customers_file)

print("Files loaded successfully.")

# cleaning step 1 (Remove quotes and trim any spaces)

clean_text_column(books_df, "Book checkout")
clean_text_column(books_df, "Book Returned")
clean_text_column(books_df, "Books")

print("quotes and removed and titles trimmed...")

# Convert to date (UK)

convert_to_date(books_df, "Book checkout")
convert_to_date(books_df, "Book Returned")

print("Date conversion complete.")

# Calc book loan period

books_df = calculate_days_between_dates(
    books_df,
    start_col="Book checkout",
    end_col="Book Returned",
    new_col="LoanPeriodDays"
)

# convert weeks to days

convert_weeks_to_days(books_df)

print("Borrow time converted.")


bad_rows = validate_book_data(books_df)

print("Number of problematic rows:", len(bad_rows))

#drop bad rows from the data

clean_books_df = books_df.drop(bad_rows.index)

# create clean files from cleaned df

clean_books_df.to_csv("Data/books_cleaned.csv", index=False)
bad_rows.to_csv("Data/books_problem_rows.csv", index=False)

customers_df.to_csv("Data/customers_cleaned.csv", index=False)

print("Cleaning complete.")
print("Cleaned files saved.")


print("Loading data to SQL Database: Newham Library...")

#------------------------------------------------------------------------
#SQL FUNCTIONS

def connect():
    return pyodbc.connect(CONN_STR)

def insert_df(conn, table_name, df):
    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join([f"[{c}]" for c in cols])

    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.fast_executemany = True

    rows = df.astype(object).where(pd.notnull(df), None).values.tolist()
    cur.executemany(sql, rows)

    conn.commit()

def insert_df_raw_strings(conn, table_name, df):

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

def prep_books_for_silver(df):
    df = df.copy()

    for col in ["Id", "CustomerID", "AllowedDays"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    for col in ["CheckoutDate", "ReturnDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df = df.astype(object).where(pd.notnull(df), None)

    return df

def main():
    # Basic file checks
    missing_required = [
        key for key in REQUIRED
        if not FILES[key].exists()
    ]

    if missing_required:
        print("Missing required files:", missing_required)
        return

    print("Required files found. Continuing...")

    conn = connect()
  
    #BRONZE LOAD
    
    books_raw = pd.read_csv(FILES["books_raw"], dtype=str)
    books_raw["SourceFile"] = FILES["books_raw"].name
    insert_df_raw_strings(conn, "bronze.books_raw", books_raw)

    customers_raw = pd.read_csv(FILES["customers_raw"], dtype=str)
    customers_raw["SourceFile"] = FILES["customers_raw"].name
    insert_df_raw_strings(conn, "bronze.customers_raw", customers_raw)

    #SILVER LOAD

    books_clean = pd.read_csv(FILES["books_clean"])

    if "Books" in books_clean.columns:
        books_clean = books_clean.rename(columns={"Books": "BookTitle"})
    if "Customer ID" in books_clean.columns:
        books_clean = books_clean.rename(columns={"Customer ID": "CustomerID"})
    if "Book checkout" in books_clean.columns:
        books_clean = books_clean.rename(columns={"Book checkout": "CheckoutDate"})
    if "Book Returned" in books_clean.columns:
        books_clean = books_clean.rename(columns={"Book Returned": "ReturnDate"})
    if "Allowed Days" in books_clean.columns:
        books_clean = books_clean.rename(columns={"Allowed Days": "AllowedDays"})

    books_clean_subset = books_clean[[
        "Id", "BookTitle", "CheckoutDate", "ReturnDate", "AllowedDays", "CustomerID"
    ]]

    books_clean_subset = prep_books_for_silver(books_clean_subset)

    insert_df(conn, "silver.books_clean", books_clean_subset)

    #AUDIT LOAD

    books_err = pd.read_csv(FILES["books_errors"], dtype=str)

    if "Books" in books_err.columns:
        books_err = books_err.rename(columns={"Books": "BookTitle"})
    if "Book checkout_raw" in books_err.columns:
        books_err = books_err.rename(columns={"Book checkout_raw": "CheckoutRaw"})
    if "Book Returned_raw" in books_err.columns:
        books_err = books_err.rename(columns={"Book Returned_raw": "ReturnRaw"})
    if "Customer ID" in books_err.columns:
        books_err = books_err.rename(columns={"Customer ID": "CustomerID"})

    audit_books = books_err.reindex(columns=[
        "Id", "BookTitle", "CheckoutRaw", "ReturnRaw", "CustomerID",
        "Invalid Checkout Date", "Invalid Return Date", "Checkout After Return"
    ], fill_value=None).rename(columns={
        "Invalid Checkout Date": "InvalidCheckoutDate",
        "Invalid Return Date": "InvalidReturnDate",
        "Checkout After Return": "CheckoutAfterReturn"
    }).assign(SourceFile=FILES["books_raw"].name)

# Convert flags to 0/1 so they insert cleanly into BIT columns
    for c in ["InvalidCheckoutDate", "InvalidReturnDate", "CheckoutAfterReturn"]:
        audit_books[c] = audit_books[c].map(lambda x: 1 if str(x).strip().lower() in ("true", "1", "yes") else 0)

    insert_df(conn, "audit.books_errors", audit_books)

    conn.close()
    print("sql tables loaded.")

if __name__ == "__main__":
    main()
