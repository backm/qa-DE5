USE NewhamLibrary
GO


-- empty rows in bronze / raw books

SELECT
    count(*) AS TotalBronzeRows,
    SUM(CASE WHEN
        nullif(ltrim(rtrim([Books])), '') IS NULL
        AND nullif(ltrim(rtrim([Book checkout])), '') IS NULL
        AND nullif(ltrim(rtrim([Book Returned])), '') IS NULL
        AND nullif(ltrim(rtrim([Days allowed to borrow])), '') IS NULL
        AND nullif(ltrim(rtrim([Customer ID])), '') IS NULL
    THEN 1 ELSE 0 END) AS BlankRows
FROM bronze.books_raw;

-- (silver) books without a customer in (silver) customers

SELECT
    count(*) AS BooksWithNoMatchingCustomer
FROM silver.books_clean b
LEFT JOIN silver.customers_clean c
    ON c.CustomerID = b.CustomerID
WHERE b.CustomerID IS NOT NULL
  AND c.CustomerID IS NULL;

  -- count bronze load

SELECT
    SourceFile,
    count(*) AS RowsLoaded,
    min(LoadDts) AS FirstSeen,
    max(LoadDts) AS LastSeen
FROM bronze.books_raw
GROUP BY SourceFile
ORDER BY LastSeen DESC, SourceFile;

-- counts_all rows instered all tables

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM bronze.books_raw)
SELECT 'bronze.books_raw' AS TableName, COUNT(*) AS RowsInserted
FROM bronze.books_raw
WHERE LoadDts = (SELECT d FROM Latest);

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM bronze.customers_raw)
SELECT 'bronze.customers_raw', COUNT(*)
FROM bronze.customers_raw
WHERE LoadDts = (SELECT d FROM Latest);

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM silver.books_clean)
SELECT 'silver.books_clean', COUNT(*)
FROM silver.books_clean
WHERE LoadDts = (SELECT d FROM Latest);

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM silver.customers_clean)
SELECT 'silver.customers_clean', COUNT(*)
FROM silver.customers_clean
WHERE LoadDts = (SELECT d FROM Latest);

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM audit.books_errors)
SELECT 'audit.books_errors', COUNT(*)
FROM audit.books_errors
WHERE LoadDts = (SELECT d FROM Latest);

;WITH Latest AS (SELECT MAX(LoadDts) AS d FROM audit.customers_errors)
SELECT 'audit.customers_errors', COUNT(*)
FROM audit.customers_errors
WHERE LoadDts = (SELECT d FROM Latest);

-- loan duration

;WITH BooksWithDays AS -- cleaner failed to write BookLoanDuration so deriving in sql
(
    SELECT
        b.*,
        CASE
            WHEN b.CheckoutDate IS NULL OR b.ReturnDate IS NULL THEN NULL
            ELSE DATEDIFF(day, b.CheckoutDate, b.ReturnDate)
        END AS NoDaysBorrowed
    FROM silver.books_clean b
)
SELECT
    COUNT(*) AS LoansWithDaysBorrowed,
    AVG(CAST(NoDaysBorrowed AS float)) AS AvgDaysBorrowed,
    MIN(NoDaysBorrowed) AS MinDaysBorrowed,
    MAX(NoDaysBorrowed) AS MaxDaysBorrowed
FROM BooksWithDays
WHERE NoDaysBorrowed IS NOT NULL;



;WITH BooksWithDays AS -- cleaner failed to write BookLoanDuration so deriving in sql
(
    SELECT
        b.*,
        CASE
            WHEN b.CheckoutDate IS NULL OR b.ReturnDate IS NULL THEN NULL
            ELSE DATEDIFF(day, b.CheckoutDate, b.ReturnDate)
        END AS NoDaysBorrowed
    FROM silver.books_clean b
)
SELECT
    COUNT(*) AS LoansOverAllowedDays
FROM BooksWithDays
WHERE NoDaysBorrowed IS NOT NULL
  AND AllowedDays IS NOT NULL
  AND NoDaysBorrowed > AllowedDays;