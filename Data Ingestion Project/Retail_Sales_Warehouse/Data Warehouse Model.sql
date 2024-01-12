-- Drop Tables If They Exist
DROP TABLE IF EXISTS RetailSalesFacts;
DROP TABLE IF EXISTS ProductDim;
DROP TABLE IF EXISTS DateDim;
DROP TABLE IF EXISTS CashierDim;
DROP TABLE IF EXISTS date_sequence;

-- Retail Sales Transaction Fact Table
CREATE TABLE RetailSalesFacts(
    Date_Key INT NOT NULL,
    Product_key INT NOT NULL ,
    Cashier_Key INT NOT NULL,
    POS_Transaction VARCHAR(10) PRIMARY KEY,
    Sales_Quantity FLOAT DEFAULT 0.0,
    Regular_Unit_Price FLOAT DEFAULT 0.0,
    CONSTRAINT valid_Sales_Quantity CHECK (Sales_Quantity > 0)
);

-- Product Dimension Table
CREATE TABLE ProductDim(
    Product_key SERIAL PRIMARY KEY,
    SKU_Number VARCHAR(10),
    Product_Description VARCHAR(20),
    Brand_Description VARCHAR(20)
);

-- Cashier Dimension Table
CREATE TABLE CashierDim(
    Cashier_Key SERIAL PRIMARY KEY,
    Cashier_Employee_ID VARCHAR(10),
    Cashier_Name VARCHAR(10)
);

-- Date Dimension Table
CREATE TABLE DateDim(
    Date_Key INT PRIMARY KEY,
    Actual_Date DATE,
    Day_Of_Week VARCHAR(5),
    Calendar_Month VARCHAR(5),
    Calendar_Year VARCHAR(5)
);

-- Create Data for DateDim
CREATE TABLE date_sequence (date DATE NOT NULL);

-- Populate date_sequence with 30 days starting from '2023-05-01'
INSERT INTO date_sequence(date)
SELECT '2023-05-01'::DATE + SEQUENCE.number AS date
FROM GENERATE_SERIES(0, 30) AS SEQUENCE (number);

-- Populate DateDim with relevant information from date_sequence
INSERT INTO DateDim
SELECT   
    TO_CHAR(date, 'yyyymmdd')::INT AS date_dim_id,   
    date AS Actual_Date,
    EXTRACT(ISODOW FROM date) AS day_of_week,
    EXTRACT(MONTH FROM date) AS month_actual,   
    EXTRACT(YEAR FROM date) AS year_actual
FROM date_sequence   
ORDER BY date ASC;

-- Define Foreign Key Constraints
ALTER TABLE RetailSalesFacts 
ADD CONSTRAINT FK_RetailSalesFacts_ProductDim FOREIGN KEY (Product_key) REFERENCES ProductDim (Product_key),
ADD CONSTRAINT FK_RetailSalesFacts_DateDim FOREIGN KEY (Date_Key) REFERENCES DateDim (Date_Key),
ADD CONSTRAINT FK_RetailSalesFacts_CashierDim FOREIGN KEY (Cashier_Key) REFERENCES CashierDim (Cashier_Key);
