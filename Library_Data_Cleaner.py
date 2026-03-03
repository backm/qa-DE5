
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
from datetime import datetime

#env requires "pip install pyodbc pandas" for SQL actions

books_file = "Data/03_Library Systembook.csv"
customers_file = "Data/03_Library SystemCustomers.csv"

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

#FUNCTIONS


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
#SQL FUNCTIONS

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

#SQL CODE
DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit') EXEC('CREATE SCHEMA audit');

-- Bronze (all strings)
IF OBJECT_ID('bronze.books_raw','U') IS NULL
BEGIN
    CREATE TABLE bronze.books_raw (
        Id NVARCHAR(255) NULL,
        Books NVARCHAR(255) NULL,
        [Book checkout] NVARCHAR(255) NULL,
        [Book Returned] NVARCHAR(255) NULL,
        [Days allowed to borrow] NVARCHAR(255) NULL,
        [Customer ID] NVARCHAR(255) NULL,
        SourceFile NVARCHAR(255) NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('bronze.customers_raw','U') IS NULL
BEGIN
    CREATE TABLE bronze.customers_raw (
        [Customer ID] NVARCHAR(255) NULL,
        [Customer Name] NVARCHAR(255) NULL,
        SourceFile NVARCHAR(255) NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

-- Silver (typed)
IF OBJECT_ID('silver.books_clean','U') IS NULL
BEGIN
    CREATE TABLE silver.books_clean (
        Id INT NULL,
        BookTitle NVARCHAR(255) NULL,
        CheckoutDate DATE NULL,
        ReturnDate DATE NULL,
        AllowedDays INT NULL,
        CustomerID INT NULL,
        DaysBorrowed INT NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('silver.customers_clean','U') IS NULL
BEGIN
    CREATE TABLE silver.customers_clean (
        CustomerID INT NULL,
        CustomerName NVARCHAR(255) NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

-- Audit / errors (store raw + flags)
IF OBJECT_ID('audit.books_errors','U') IS NULL
BEGIN
    CREATE TABLE audit.books_errors (
        Id NVARCHAR(255) NULL,
        BookTitle NVARCHAR(255) NULL,
        CheckoutRaw NVARCHAR(255) NULL,
        ReturnRaw NVARCHAR(255) NULL,
        CustomerID NVARCHAR(255) NULL,
        InvalidCheckoutDate BIT NULL,
        InvalidReturnDate BIT NULL,
        CheckoutAfterReturn BIT NULL,
        SourceFile NVARCHAR(255) NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID('audit.customers_errors','U') IS NULL
BEGIN
    CREATE TABLE audit.customers_errors (
        CustomerID NVARCHAR(255) NULL,
        CustomerName NVARCHAR(255) NULL,
        Reason NVARCHAR(255) NULL,
        SourceFile NVARCHAR(255) NULL,
        LoadDts DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
"""

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

#SQL connect
def connect():
    return pyodbc.connect(CONN_STR)

def run_ddl(conn):
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

def insert_df(conn, table_name, df):
    # Simple row-by-row insert (fine for small datasets)
    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join([f"[{c}]" for c in cols])

    sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, df.where(pd.notnull(df), None).values.tolist())
    conn.commit()

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
    run_ddl(conn)

    #BRONZE LOAD
    
    books_raw = pd.read_csv(FILES["books_raw"], dtype=str)
    books_raw["SourceFile"] = FILES["books_raw"].name
    insert_df(conn, "bronze.books_raw", books_raw)

    customers_raw = pd.read_csv(FILES["customers_raw"], dtype=str)
    customers_raw["SourceFile"] = FILES["customers_raw"].name
    insert_df(conn, "bronze.customers_raw", customers_raw)

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


    insert_df(conn, "silver.books_clean", books_clean[[
        "Id", "BookTitle", "CheckoutDate", "ReturnDate", "Allowed Days", "CustomerID"
    ]].rename(columns={"Allowed Days": "AllowedDays"}))

    customers_clean = pd.read_csv(FILES["customers_clean"])
    customers_clean = customers_clean.rename(columns={
        "Customer ID": "CustomerID",
        "Customer Name": "CustomerName"
    })
    insert_df(conn, "silver.customers_clean", customers_clean[["CustomerID", "CustomerName"]])

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

    insert_df(conn, "audit.books_errors", books_err.reindex(columns=[
        "Id", "BookTitle", "CheckoutRaw", "ReturnRaw", "CustomerID",
        "Invalid Checkout Date", "Invalid Return Date", "Checkout After Return"
    ], fill_value=None).rename(columns={
        "Invalid Checkout Date": "InvalidCheckoutDate",
        "Invalid Return Date": "InvalidReturnDate",
        "Checkout After Return": "CheckoutAfterReturn"
    }).assign(SourceFile=FILES["books_raw"].name))



    conn.close()
    print("sql tables loaded.")

