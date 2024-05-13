CREATE SCHEMA IF NOT EXISTS BL_3NF;

SET SEARCH_PATH TO BL_3NF;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_3nf') THEN
        CREATE ROLE BL_3NF;
    END IF;
END $$;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  BL_3NF.Log_table (
    LogID bigserial PRIMARY KEY,
    LogDateTime timestamp,
    ProcedureName VARCHAR(255),
    NumRowsAffected bigINT,
    LogMessage text,
    Load_id int
);


CREATE or replace PROCEDURE BL_3nf.InsertLog(
    IN procedureName VARCHAR(255),
    IN numRowsAffected bigINT,
    IN logMessage text,
    IN i_load_id int
)
as $$
begin
    INSERT INTO BL_3NF.Log_table (LogDateTime, ProcedureName, NumRowsAffected, LogMessage, Load_id)
    VALUES (cast(NOW() as TIMESTAMP), procedureName, numRowsAffected, logMessage, i_load_id);
commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Create the Dim_Categories table if it does not exist

CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Categories (
    PK_Category_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Category text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);


-- Create the Dim_Sectors table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Sectors (
    PK_Sector_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Sector text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);


-- Create the Dim_Segments table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Segments (
    PK_Segment_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Segment text   not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);


-- Create the Dim_Markets table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Markets (
    PK_Market_ID BIGSERIAL PRIMARY KEY,
    Market text NOT NULL UNIQUE,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Regions table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Regions (
    PK_Region_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Region text NOT NULL UNIQUE,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Countries table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Countries (
    PK_Country_ID BIGSERIAL PRIMARY KEY,
    FK_Region_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Country text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Orders table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Orders (
    PK_Order_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Order_Number text not null unique,
    Ship_Date DATE,
    Order_Priority TEXT,
    Ship_Mode TEXT
);

-- Create the Dim_Addresses table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Addresses (
    PK_Address_ID BIGSERIAL PRIMARY KEY,
    FK_Country BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Shipping_Address text not null unique,
    City TEXT,
    Postal_Code TEXT,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Subcategories table if it does not exist

CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Subcategories (
    PK_Subcategory_ID BIGSERIAL PRIMARY KEY,
    FK_Category_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Subcategory text  not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);


-- Create a table for dimension representing Products with slowly changing dimension type 2 (SCD2)
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Products_SCD2 (
    PK_Product_ID BIGSERIAL PRIMARY KEY,
    FK_Subcategory_ID BIGSERIAL ,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Product_NR text,
    Product_name TEXT,
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE BOOLEAN
);


CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Customers_SCD2 (
    PK_Customer_ID BIGSERIAL PRIMARY KEY,
    FK_Sector_ID BIGSERIAL,
    FK_Segment_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Customer_NR VARCHAR(255),
    Tax_Number VARCHAR(255),
    Contact_person VARCHAR(255),
    Customer_Name VARCHAR(255),
    Email VARCHAR(255),
    Gender VARCHAR(255),
    Age_Group VARCHAR(255),
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE BOOLEAN
);


-- Create the Dim_Employees_SCD2 table if it does not exist
CREATE TABLE IF NOT EXISTS BL_3NF.Dim_Employees_SCD2 (
    PK_Employee_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Employee_NR TEXT ,
    Employee_FirstName TEXT,
    Employee_LastName TEXT,
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE BOOLEAN
);


--I created the fact table. There are two inserts in the fact table because there are two data sources.
CREATE TABLE IF NOT EXISTS BL_3NF.FCT_ORDERS_DD (
    FK_Employee_ID BIGSERIAL,
    FK_Customer_ID BIGSERIAL,
    FK_Shipping_Address_ID BIGSERIAL,
    FK_Product_ID BIGSERIAL,
    FK_Order_ID BIGSERIAL,
    FK_Market_ID BIGSERIAL,
    Order_Date date,
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    Shipping_Cost FLOAT,
    load_id int,
    PRIMARY KEY (FK_Employee_ID, FK_Customer_ID, FK_Shipping_Address_ID, FK_Product_ID, FK_Order_ID, FK_Market_ID)
);

-----------------Insert default value in the dimensions---------------------------------------------------------------------
create or replace procedure init_dimensions()
as $$
begin
INSERT INTO BL_3NF.Dim_Categories(PK_Category_ID, Source_system, Source_entity, Source_id, Category, INSERT_DT, UPDATE_DT)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (Category) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Regions(PK_Region_ID, Source_system, Source_entity, Source_id, Region, INSERT_DT, UPDATE_DT)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (Region) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Countries(PK_Country_ID, Source_system, Source_entity, Source_id, Country, INSERT_DT, UPDATE_DT)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (Country) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Orders(PK_Order_ID, Source_system, Source_entity, Source_id, Order_Number, Ship_Date, Order_Priority)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'),'NA')
ON CONFLICT (PK_Order_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Subcategories(PK_Subcategory_ID, Source_system, Source_entity, Source_id, Subcategory, INSERT_DT, UPDATE_DT)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (Subcategory) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Categories(PK_Category_ID, Source_system, Source_entity, Source_id, Category, INSERT_DT, UPDATE_DT)
values (-1,'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (Category) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Sectors(PK_Sector_ID, Source_system, Source_entity, Source_id, Sector, INSERT_DT, UPDATE_DT)
VALUES ( -1,'GLOBAL_SUPERSTORE', 'MANUAL','NA','NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (PK_Sector_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Markets(PK_Market_ID,Market, Source_system, Source_entity, Source_id, INSERT_DT, UPDATE_DT)
values ( -1,'NA','GLOBAL_SUPERSTORE', 'MANUAL','NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (PK_Market_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Segments(PK_Segment_ID, Source_system, Source_entity, Source_id, Segment, INSERT_DT, UPDATE_DT)
values ( -1,'GLOBAL_SUPERSTORE', 'MANUAL','NA','NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (PK_Segment_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Addresses(PK_Address_ID,FK_Country, Source_system, Source_entity, Source_id, Shipping_Address, City, Postal_Code, INSERT_DT, UPDATE_DT)
VALUES (-1, -1, 'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', 'NA', 'NA', TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'))
ON CONFLICT (PK_Address_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Products_SCD2(PK_Product_ID, FK_Subcategory_ID, Source_system, Source_entity, Source_id, Product_NR, Product_name, START_DT, END_DT, IS_ACTIVE)
values (-1,-1, 'GLOBAL_SUPERSTORE', 'MANUAL', 'NA', 'NA', 'NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'),  TO_DATE('9999-12-31', 'YYYY-MM-DD'), TRUE)
ON CONFLICT (PK_Product_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Customers_SCD2(PK_Customer_ID, Source_system, Source_entity, Source_id, Customer_NR, Contact_person, Customer_Name,Email,Gender,Age_Group,START_DT,END_DT,IS_ACTIVE)
values ( -1,'GLOBAL_SUPERSTORE', 'MANUAL','NA','NA','NA','NA','NA','NA','NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'), TRUE)
ON CONFLICT (PK_Customer_ID) DO NOTHING;
COMMIT;
INSERT INTO BL_3NF.Dim_Employees_SCD2(PK_Employee_ID, Source_system, Source_entity, Source_id, Employee_NR, Employee_FirstName, Employee_LastName,START_DT,END_DT,IS_ACTIVE)
values ( -1,'GLOBAL_SUPERSTORE', 'MANUAL','NA','NA','NA','NA',TO_DATE('1900-01-01', 'YYYY-MM-DD'), TO_DATE('9999-12-31', 'YYYY-MM-DD'), TRUE)
ON CONFLICT (PK_Employee_ID) DO NOTHING;
COMMIT;
end;
$$ language plpgsql;

call init_dimensions();

GRANT CREATE ON SCHEMA bl_3nf TO bl_cl;
grant execute on procedure InsertLog to bl_cl;
grant select, insert, update on all tables in schema bl_3nf to bl_cl;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA bl_3nf TO bl_cl;
