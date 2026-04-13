create database Book_DWH;

USE Book_DWH;
GO

create table CustomerDim(
  CustomerID int primary key,
  FullName varchar(50),
  City varchar(50),
)

create table BookDim(
  Book_SK int identity(1,1) primary key,
  BookID int,
  Title varchar(50),
  CategoryDescription varchar(100),
  AuthorName varchar(100),
  Price float,
  BookAge float,
)

truncate table DateDim
create table DateDim
(
  Date_SK int primary key,
  Full_Date date not null,
  Year int,
  Quarter int ,
  Month_Number int,
  Month_Name nvarchar(50),
  Day_of_Week int,
  Day_Name nvarchar(50),
  Day_of_Month int,
  Week_of_Year int,
  Is_Weekend bit
)

drop table FactOrders
create table FactOrders(
  SalesID int identity(1,1) primary key,
  OrderID int,
  CustomerID int foreign key references CustomerDim(CustomerID),
  BookID int foreign key references BookDim(Book_SK),
  OrderDate int foreign key references DateDim(Date_SK),
  Quantity int,
  UnitPrice float,
  TotalAmount float,
)

insert into CustomerDim values (0, 'Gust Account', 'Unknown')
select * from CustomerDim;
select * from BookDim;
select * from DateDim;

-- Simple stored procedure to populate date dimension
CREATE PROCEDURE sp_PopulateDateDimension
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    DECLARE @CurrentDate DATE = @StartDate;
    
    WHILE @CurrentDate <= @EndDate
    BEGIN
        INSERT INTO DateDim(
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
EXEC sp_PopulateDateDimension '2020-01-01', '2030-12-31';

truncate table FactOrders
select * from FactOrders;