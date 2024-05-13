create schema if not exists sa_SuperStore_Consumer;
set SEARCH_PATH to sa_SuperStore_Consumer;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sa_superstore_consumer') THEN
        CREATE ROLE sa_SuperStore_Consumer;
    END IF;
END $$;

CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS SuperStore FOREIGN DATA WRAPPER file_fdw;

/*I created the external table to access the data from the Global_Superstore_consumer.csv file*/

drop foreign table if exists sa_SuperStore_Consumer.ext_Consumer ;

CREATE FOREIGN TABLE IF NOT EXISTS ext_Consumer (
    Row_ID VARCHAR(255),
    Order_Number VARCHAR(255),
    Employee_FirstName VARCHAR(255),
    Employee_LastName VARCHAR(255),
    Employee_Nr VARCHAR(255),
    Order_Date VARCHAR(255),
    Ship_Date VARCHAR(255),
    Ship_Mode VARCHAR(255),
    Customer_Nr VARCHAR(255),
    Customer_Name VARCHAR(255),
    Address VARCHAR(255),
    Gender VARCHAR(255),
    Age_Group VARCHAR(255),
    Email VARCHAR(255),
    Segment VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Postal_Code VARCHAR(255),
    Market VARCHAR(255),
    Product_Nr VARCHAR(255),
    Category_Name VARCHAR(255),
    Sub_Category_Name VARCHAR(255),
    Product_Name VARCHAR(255),
    Sales VARCHAR(255),
    Quantity VARCHAR(255),
    Discount VARCHAR(255),
    Profit VARCHAR(255),
    Shipping_Cost VARCHAR(255),
    Order_Priority VARCHAR(255)
) SERVER SuperStore
OPTIONS ( filename 'C:\CSV/Global_Superstore_consumer.csv', format 'csv', delimiter ',', header 'true' );


/*I created the scr_Consumer source table to store the input from the external table*/

CREATE TABLE IF NOT EXISTS scr_Consumer(    Row_ID VARCHAR(255),
    Order_Number VARCHAR(255),
    Employee_FirstName VARCHAR(255),
    Employee_LastName VARCHAR(255),
    Employee_Nr VARCHAR(255),
    Order_Date VARCHAR(255),
    Ship_Date VARCHAR(255),
    Ship_Mode VARCHAR(255),
    Customer_Nr VARCHAR(255),
    Customer_Name VARCHAR(255),
    Address VARCHAR(255),
    Gender VARCHAR(255),
    Age_Group VARCHAR(255),
    Email VARCHAR(255),
    Segment VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Postal_Code VARCHAR(255),
    Market VARCHAR(255),
    Product_Nr VARCHAR(255),
    Category_Name VARCHAR(255),
    Sub_Category_Name VARCHAR(255),
    Product_Name VARCHAR(255),
    Sales VARCHAR(255),
    Quantity VARCHAR(255),
    Discount VARCHAR(255),
    Profit VARCHAR(255),
    Shipping_Cost VARCHAR(255),
    Order_Priority VARCHAR(255),
    Insert_Date VARCHAR(255),
    Load_id VARCHAR(255));



/*Grant is issued to bl_cl and bl_3nf shemas*/

GRANT select, insert, update, delete ON ALL TABLES IN SCHEMA sa_SuperStore_Consumer TO bl_cl;
GRANT select, insert, update, delete ON ALL TABLES IN SCHEMA sa_SuperStore_Consumer TO bl_3nf;
