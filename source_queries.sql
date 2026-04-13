use BookStore_EG;
go

create view customer_view as
Select CustomerID, CONCAT(FirstName,' ',LastName) as FullName, City
from Customer;

select * from customer_view;

CREATE VIEW book_view AS
SELECT b.BookID,Title,CategoryDescription,AuthorName,Price,YEAR(GETDATE()) - [Year] AS BookAge
FROM Book b
LEFT JOIN Category c 
ON b.CategoryID = c.CategoryID
left outer join Author_Book ab
on b.BookID = ab.BookID
left outer join Author o
on ab.AuthorID = o.AuthorID;


select * from book_view;

create view fact_view as
select  o.orderID,CustomerID, BookID, OrderDate, Quantity, Price as UnitPrice
from
Ordering o left outer join Book_Order b
on o.OrderID = b.OrderID

select * from fact_view;

-- Simple stored procedure to populate date dimension
CREATE PROCEDURE sp_PopulateDateDimension
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    DECLARE @CurrentDate DATE = @StartDate;
    
    WHILE @CurrentDate <= @EndDate
    BEGIN
        INSERT INTO Dim_Date (
            Date_SK,
            Full_Date,
            Year,
            Quarter,
            Month_Number,
            Month_Name,
            Day_of_Week,
            Day_Name,
            Day_of_Month,
            Week_of_Year,
            Is_Weekend
        )
        VALUES (
            CONVERT(INT, FORMAT(@CurrentDate, 'yyyyMMdd')),
            @CurrentDate,
            YEAR(@CurrentDate),
            DATEPART(QUARTER, @CurrentDate),
            MONTH(@CurrentDate),
            DATENAME(MONTH, @CurrentDate),
            DATEPART(WEEKDAY, @CurrentDate),
            DATENAME(WEEKDAY, @CurrentDate),
            DAY(@CurrentDate),
            DATEPART(WEEK, @CurrentDate),
            CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END
        );
        
        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END
END;
GO


-- Populate dimensions
EXEC sp_PopulateDateDimension '2025-01-01', '2030-12-31';