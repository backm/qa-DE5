
--  data quality issues
--  no. books with missing cust



-- useful library stats



SELECT TOP (1000) [Id]
      ,[BookTitle]
      ,[CheckoutDate]
      ,[ReturnDate]
      ,[AllowedDays]
      ,b.[CustomerID]
      ,datediff(day,[CheckoutDate],[ReturnDate]) as Days_Borrowed
      ,b.[LoadDts]
	  ,c.CustomerName
  FROM [NewhamLibrary].[silver].[books_clean] b

  LEFT JOIN silver.customers_clean c on c.CustomerID = b.CustomerID
