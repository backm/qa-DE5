#Library Data File  Cleaner v1.0

##Overview

This project contains a small Python tool that cleans two CSV files containing library book and customer data.

The raw data contains issues such as:

- Invalid dates (e.g. 32/05/2023)
- Missing dates
- Checkout dates that occur after return dates
- Borrow periods stored as text (e.g. "2 weeks")

The cleaning script fixes and standardises the data, flags problematic rows, and outputs new cleaned CSV files.

##What the Cleaning Script Does

__The script:__

- Loads the raw CSV files
- Cleans date columns
- Removes quotes and extra spaces
- Converts UK dates (DD/MM/YYYY) to proper datetime format
- Converts "Days allowed to borrow" from text (e.g. "2 weeks") into a numeric column (Days)
- Flags data issues:
    - Invalid checkout dates
    - Invalid return dates
    - Checkout date occurring after return date
- Outputs:
    - A cleaned books file
    - A file containing only problematic rows
    - A cleaned customers file

##How to Run the Cleaner

Run (python):

    __Library_Data_Cleaner.py__

The cleaned files will be saved into the __Data__ folder.


##Requirements

Python 3.x

pyodbc pandas 

pandas
os
sys

files to be held in folder "Data" with naming conventions:

- "03_Library Systembook.csv"
- "03_Library SystemCustomers.csv"


#SQL

##Schemas

bronze = raw ingested, everything as text (no assumptions)

silver = cleaned/typed, plus derived fields

audit = errors / rejects / load logs

##Tables

Bronze

bronze.books_raw

bronze.customers_raw

Silver

silver.books_clean

silver.customers_clean

Audit

audit.books_errors