CREATE SCHEMA IF NOT EXISTS sa_SuperStore_Corporate;
SET SEARCH_PATH TO sa_SuperStore_Corporate;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sa_superstore_corporate') THEN
        CREATE ROLE sa_SuperStore_Corporate;
    END IF;
END $$;


CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS SuperStore FOREIGN DATA WRAPPER file_fdw;

/*I created the external table to access the data from the Global_Superstore_corp_home.csv file*/
drop foreign table if exists sa_SuperStore_Corporate.ext_Corporate ;

CREATE FOREIGN TABLE IF NOT EXISTS ext_Corporate (
    Row_ID VARCHAR(255),
    Order_ID VARCHAR(255),
    Employee_Name VARCHAR(255),
    Employee_ID VARCHAR(255),
    Order_Date VARCHAR(255),
    Shipping VARCHAR(255),
    Ship_Mode VARCHAR(255),
    Customer_ID VARCHAR(255),
    Tax_Number VARCHAR(255),
    Sector VARCHAR(255),
    Contact_Person VARCHAR(255),
    Shipping_Address VARCHAR(255),
    Email VARCHAR(255),
    Segment VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Postal_Code VARCHAR(255),
    Market VARCHAR(255),
    Region VARCHAR(255),
    Product_ID VARCHAR(255),
    Category VARCHAR(255),
    Sub_Category VARCHAR(255),
    Product VARCHAR(255),
    Sales VARCHAR(255),
    Quantity VARCHAR(255),
    Discount VARCHAR(255),
    Profit VARCHAR(255),
    Shipping_Cost VARCHAR(255),
    Order_Priority VARCHAR(255)
) SERVER SuperStore
OPTIONS ( filename 'C:\CSV/Global_Superstore_corp_home.csv', format 'csv', header 'true', delimiter ','  );

/*I created the src_Coporate source table to store the input from the external table*/




create table if not exists scr_Corporate(
    Row_ID VARCHAR(255),
    Order_ID VARCHAR(255),
    Employee_Name VARCHAR(255),
    Employee_ID VARCHAR(255),
    Order_Date VARCHAR(255),
    Shipping VARCHAR(255),
    Ship_Mode VARCHAR(255),
    Customer_ID VARCHAR(255),
    Tax_Number VARCHAR(255),
    Sector VARCHAR(255),
    Contact_Person VARCHAR(255),
    Shipping_Address VARCHAR(255),
    Email VARCHAR(255),
    Segment VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    Postal_Code VARCHAR(255),
    Market VARCHAR(255),
    Region VARCHAR(255),
    Product_ID VARCHAR(255),
    Category VARCHAR(255),
    Sub_Category VARCHAR(255),
    Product VARCHAR(255),
    Sales VARCHAR(255),
    Quantity VARCHAR(255),
    Discount VARCHAR(255),
    Profit VARCHAR(255),
    Shipping_Cost VARCHAR(255),
    Order_Priority VARCHAR(255),
    Insert_Date VARCHAR(255),
    Load_id VARCHAR(255));

/*Grant is issued to bl_cl and bl_3nf shemas*/


GRANT select, insert, update, delete ON ALL TABLES IN SCHEMA sa_SuperStore_Corporate TO bl_cl;
GRANT select, insert, update, delete ON ALL TABLES IN SCHEMA sa_SuperStore_Corporate TO bl_3nf;
