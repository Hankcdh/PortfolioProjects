

--Drop Table If Exist
DROP TABLE IF EXISTS RetailSalesFacts;
DROP TABLE IF EXISTS ProductDim;
DROP TABLE IF EXISTS DateDim;
DROP TABLE IF EXISTS CashierDim;
DROP TABLE IF EXISTS date_sequence;
 


-- Retail Sales Transaction Fact Tables 
CREATE TABLE RetailSalesFacts(
    Date_Key INT,
    Product_key VARCHAR(10),
    Store_Key VARCHAR(10),
    Promotion_key VARCHAR (10),
    Cashier_Key VARCHAR (10),
    Payment_Method_key VARCHAR (10),
    POS_Transaction VARCHAR(10),
    Sales_Quantity float,
    Regular_Unit_Price float,
    Discount_Unit_Price float,
    Net_Unit_Price float,
    Extended_Discount_Dollar_Amount float,
    Extended_Sales_Dollar_Amount float,
    Extended_Cost_Amount float, 
    Extended_Gross_Profit_Dollar_Amount float
    
    
);

CREATE TABLE ProductDim(
    Product_key INT,
    SKU_Numer VARCHAR(10),
    Product_Description VARCHAR(20),
    Brand_Description VARCHAR(20)
);


CREATE TABLE CashierDim(
    Cashier_Key INT,
    Cashier_Employee_ID VARCHAR(10),
    Cashier_Name VARCHAR(10)
    
);


CREATE TABLE DateDim(
    Date_Key INT,
    Actual_Date Date,
    Day_Of_Week VARCHAR(5),
    Calender_Month VARCHAR(5),
    Calenger_Year VARCHAR(5)
);


-- Create Data for DateDim

CREATE TABLE date_sequence (date date NOT NULL);

INSERT INTO
  date_sequence(date)
  
SELECT
  '2023-05-01'::DATE + SEQUENCE.number AS date
FROM
  GENERATE_SERIES(0, 30) AS SEQUENCE (number);


INSERT INTO DateDim
SELECT   
    TO_CHAR(date, 'yyyymmdd')::INT AS date_dim_id,   
    date AS Actual_Date,
    EXTRACT(ISODOW FROM date) AS day_of_week,
    EXTRACT(MONTH FROM date) AS month_actual,   
    EXTRACT(YEAR FROM date) AS year_actual
FROM date_sequence   
ORDER BY date ASC;