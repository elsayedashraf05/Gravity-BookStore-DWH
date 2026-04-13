# 📚 Gravity BookStore — End-to-End Data Warehouse Project

> A complete Data Warehousing project built during the **DEPI Internship Program**, covering pipeline design, star schema modeling, ETL implementation, and dimensional data loading using Python and SQL Server.

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Project Steps](#project-steps)
  - [Step 1 — Pipeline Design](#step-1--pipeline-design)
  - [Step 2 — DWH Schema Design](#step-2--dwh-schema-design)
  - [Step 3 — Data Mapping](#step-3--data-mapping)
  - [Step 4 — DDL Implementation](#step-4--ddl-implementation)
  - [Step 5 — ETL Using Python](#step-5--etl-using-python)
- [Repository Structure](#repository-structure)
- [How to Run](#how-to-run)
- [Author](#author)

---

## Project Overview

This project implements a **Gravity BookStore Data Warehouse** based on a transactional OLTP system (`BookStore_EG`). The goal is to transform raw operational data into an analytics-ready **Star Schema** (`Book_DWH`) that enables efficient reporting on sales performance, customer behavior, and book popularity.

**Source System (BookStore_EG)** contains:
- Books (title, ISBN, price, year, pages)
- Authors (name, nationality, active status)
- Categories
- Customers (name, email, phone, city, segment)
- Orders & Order Details (quantity, unit price, total amount)

**Target System (Book_DWH)** is a Star Schema with:
- `CustomerDim` — customer demographics
- `BookDim` — book details with category and author hierarchy
- `DateDim` — date dimension (2020–2030)
- `FactOrders` — sales transactions (measures: Quantity, UnitPrice, TotalAmount)

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **SQL Server Express** | Source OLTP & Target DWH |
| **Python 3** | ETL scripting |
| **pandas** | Data extraction & transformation |
| **pyodbc** | Database connectivity |
| **Jupyter Notebook** | ETL development & documentation |
| **Excel** | Source-to-target mapping sheet |

---

## Project Steps

### Step 1 — Pipeline Design

The ETL pipeline follows a classic **Extract → Transform → Load** architecture:

```
 ┌──────────────────┐     Extract      ┌───────────────────┐     Load     ┌──────────────────┐
 │   BookStore_EG   │ ───────────────► │  Python / pandas  │ ───────────► │    Book_DWH      │
 │  (SQL Server)    │                  │  (Transform Layer) │              │  (Star Schema)   │
 └──────────────────┘                  └───────────────────┘              └──────────────────┘
```

**Extract:** Three SQL views (`customer_view`, `book_view`, `fact_view`) pre-join and flatten the OLTP tables before data is pulled into Python DataFrames.

**Transform:** Data cleaning steps applied per entity:
- Customers: full name concatenation, `City` nulls filled with `'Unknown'`
- Books: `AuthorName` nulls filled with `'Unknown'`, `BookAge` cast to `float`
- FactOrders: `CustomerID` nulls filled with `0` (guest account), `Quantity` nulls defaulted to `1`, `UnitPrice` nulls back-filled from `BookDim.Price` via merge, `TotalAmount` computed as `Quantity × UnitPrice`, `OrderDate` converted to integer key `YYYYMMDD`

**Load:** Row-by-row insert into each DWH dimension/fact table via `pyodbc` cursor. `BookDim` is re-queried after insert to obtain the generated surrogate key (`Book_SK`) which is then joined back to FactOrders before loading.

> 📸 *Add your pipeline diagram image here:*
> `![Pipeline Design](images/pipeline_design.png)`

---

### Step 2 — DWH Schema Design

The data warehouse follows a **Star Schema** with one central fact table surrounded by three dimension tables.

```
              ┌──────────────────────┐
              │     CustomerDim      │
              │──────────────────────│
              │ CustomerID (PK)      │
              │ FullName             │
              │ City                 │
              └──────────┬───────────┘
                         │
┌─────────────────┐  ┌───▼───────────────────┐  ┌──────────────────────┐
│    BookDim      │  │      FactOrders        │  │       DateDim        │
│─────────────────│  │────────────────────────│  │──────────────────────│
│ Book_SK (PK)    │◄─│ SalesID (PK surrogate) │─►│ Date_SK (PK)         │
│ BookID          │  │ OrderID                │  │ Full_Date            │
│ Title           │  │ CustomerID (FK)        │  │ Year / Quarter       │
│ CategoryDesc    │  │ BookID (FK → Book_SK)  │  │ Month_Number         │
│ AuthorName      │  │ OrderDate (FK)         │  │ Month_Name           │
│ Price           │  │ Quantity               │  │ Day_of_Week          │
│ BookAge         │  │ UnitPrice              │  │ Day_Name             │
└─────────────────┘  │ TotalAmount            │  │ Day_of_Month         │
                     └────────────────────────┘  │ Week_of_Year         │
                                                  │ Is_Weekend           │
                                                  └──────────────────────┘
```

> 📸 *Add your schema diagram screenshot here:*
> `![DWH Schema](images/dwh_schema.png)`

---

### Step 3 — Data Mapping

The mapping document (`Maping.xlsx`) defines the full **source-to-target column lineage** for each dimension and the fact table.

| Target Table | Target Column | Source Table(s) | Source Column | Transformation |
|---|---|---|---|---|
| CustomerDim | CustomerID | Customer | CustomerID | Direct |
| CustomerDim | FullName | Customer | FirstName, LastName | `CONCAT(FirstName, ' ', LastName)` |
| CustomerDim | City | Customer | City | Fill nulls → `'Unknown'` |
| BookDim | BookID | Book | BookID | Direct |
| BookDim | Title | Book | Title | Direct |
| BookDim | CategoryDescription | Category | CategoryDescription | Left Join on CategoryID |
| BookDim | AuthorName | Author | AuthorName | Left Join via Author_Book; Fill nulls → `'Unknown'` |
| BookDim | Price | Book | Price | Direct |
| BookDim | BookAge | Book | Year | `YEAR(GETDATE()) - Year` |
| DateDim | Date_SK | — | — | `FORMAT(date, 'yyyyMMdd')` as INT |
| DateDim | Full_Date / Year / Quarter / Month / Day / Is_Weekend | — | — | Stored procedure `sp_PopulateDateDimension` (2020–2030) |
| FactOrders | OrderID | Ordering | OrderID | Direct |
| FactOrders | CustomerID | Ordering | CustomerID | Fill nulls → `0` (guest account) |
| FactOrders | BookID | BookDim | Book_SK | Merge on BookID to get surrogate key |
| FactOrders | OrderDate | Ordering | OrderDate | `strftime('%Y%m%d').astype(int)` |
| FactOrders | Quantity | Book_Order | Quantity | Fill nulls → `1` |
| FactOrders | UnitPrice | Book_Order | Price | Fill nulls → `BookDim.Price` via merge |
| FactOrders | TotalAmount | Derived | — | `Quantity × UnitPrice` |

> 📎 Full mapping file: [`Maping.xlsx`](Maping.xlsx)

---

### Step 4 — DDL Implementation

The DWH schema was implemented using SQL Server DDL statements. Key design decisions:

- `FactOrders` uses a **surrogate key** (`SalesID IDENTITY`) since `OrderID` repeats across order lines
- `BookDim` also uses a **surrogate key** (`Book_SK IDENTITY`) to handle potential book data changes
- `OrderDate` in FactOrders is an `INT` foreign key referencing `DateDim.Date_SK` (format: `YYYYMMDD`)
- `DateDim` is pre-populated from **2020 to 2030** using a stored procedure `sp_PopulateDateDimension`
- A default **guest account** row `(0, 'Guest Account', 'Unknown')` is pre-inserted in `CustomerDim` to handle null CustomerIDs in orders

**Files:**
- [`book_dwh.sql`](book_dwh.sql) — Full DDL for all tables + stored procedure + date dimension population
- [`source_queries.sql`](source_queries.sql) — OLTP source views used during extraction

> 📸 *Add your SQL Server database diagram screenshot here:*
> `![Database Diagram](images/db_diagram.png)`

> 📸 *Add screenshots of each populated DWH table here:*
> `![CustomerDim Table](images/customer_dim.png)`
> `![BookDim Table](images/book_dim.png)`
> `![DateDim Table](images/date_dim.png)`
> `![FactOrders Table](images/fact_orders.png)`

---

### Step 5 — ETL Using Python

The ETL process is implemented in a **Jupyter Notebook** (`etl.ipynb`) with three clearly separated stages:

#### Extract

```python
import pyodbc
import pandas as pd

source_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\SQLEXPRESS;"
    "DATABASE=BookStore_EG;"
    "Trusted_Connection=yes;"
)

customer_df   = pd.read_sql("SELECT * FROM customer_view", source_conn)
book_df       = pd.read_sql("SELECT * FROM book_view", source_conn)
factorders_df = pd.read_sql("SELECT * FROM fact_view", source_conn)
```

#### Transform

```python
# --- CustomerDim ---
customer_df['City'] = customer_df['City'].fillna('Unknown')

# --- BookDim ---
book_df['AuthorName'] = book_df['AuthorName'].fillna('Unknown')
book_df['BookAge']    = book_df['BookAge'].astype(float)

# --- FactOrders ---
factorders_df['CustomerID'] = factorders_df['CustomerID'].fillna(0).astype(int)
factorders_df['Quantity']   = factorders_df['Quantity'].fillna(1).astype(int)

# Back-fill UnitPrice from BookDim.Price
factorders_df = factorders_df.merge(book_df[['BookID', 'Price']], on='BookID', how='left')
factorders_df['UnitPrice'] = factorders_df['UnitPrice'].fillna(factorders_df['Price'])
factorders_df.drop('Price', axis=1, inplace=True)

# Derive TotalAmount
factorders_df['TotalAmount'] = factorders_df['Quantity'] * factorders_df['UnitPrice']

# Convert OrderDate to integer date key (YYYYMMDD)
factorders_df['OrderDate'] = pd.to_datetime(factorders_df['OrderDate'], errors='coerce')
factorders_df['OrderDate'] = factorders_df['OrderDate'].dt.strftime('%Y%m%d').astype(int)
```

#### Load

```python
destination_conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\SQLEXPRESS;"
    "DATABASE=Book_DWH;"
    "Trusted_Connection=yes;"
)

# Load CustomerDim
for index, row in customer_df.iterrows():
    cursor.execute(
        "INSERT INTO CustomerDim (CustomerID, FullName, City) VALUES (?, ?, ?)",
        row['CustomerID'], row['FullName'], row['City']
    )

# Load BookDim, then re-query to get surrogate keys (Book_SK)
book_dim = pd.read_sql("SELECT * FROM BookDim", destination_conn)
factorders_df = pd.merge(factorders_df, book_dim, how='inner', on='BookID')
factorders_df = factorders_df[['orderID','CustomerID','Book_SK','OrderDate','Quantity','UnitPrice','TotalAmount']]

# Load FactOrders using Book_SK as the FK
for index, row in factorders_df.iterrows():
    cursor.execute(
        "INSERT INTO FactOrders (OrderID, CustomerID, BookID, OrderDate, Quantity, UnitPrice, TotalAmount) VALUES (?, ?, ?, ?, ?, ?, ?)",
        row['orderID'], row['CustomerID'], row['Book_SK'], row['OrderDate'], row['Quantity'], row['UnitPrice'], row['TotalAmount']
    )
```

> 📓 Full notebook: [`etl.ipynb`](etl.ipynb)

---

## Repository Structure

```
Gravity-BookStore-DWH/
│
├── README.md                  # This file
├── etl.ipynb                  # ETL pipeline (Extract, Transform, Load)
├── book_dwh.sql               # DWH DDL — tables, stored procedure, date dimension
├── source_queries.sql         # OLTP source views for extraction
├── Maping.xlsx                # Source-to-target mapping sheet
│
└── images/                    # Screenshots (add yours here)
    ├── pipeline_design.png
    ├── dwh_schema.png
    ├── db_diagram.png
    ├── customer_dim.png
    ├── book_dim.png
    ├── date_dim.png
    └── fact_orders.png
```

---

## How to Run

**Prerequisites:**
- SQL Server Express with ODBC Driver 17
- Python 3.x
- Jupyter Notebook
- `pip install pyodbc pandas`

**Steps:**

1. **Restore OLTP database** — Restore `books.bak` into SQL Server as `BookStore_EG`
2. **Create DWH** — Run `book_dwh.sql` to create `Book_DWH` with all tables and the date dimension
3. **Create source views** — Run `source_queries.sql` on `BookStore_EG` to create the extraction views
4. **Run ETL** — Open and execute `etl.ipynb` in Jupyter Notebook

---

## Author

**ESAB**
DEPI Internship Program — Data Engineering Track
📅 April 2026

---

> ⭐ If you found this project useful, feel free to star the repository!
