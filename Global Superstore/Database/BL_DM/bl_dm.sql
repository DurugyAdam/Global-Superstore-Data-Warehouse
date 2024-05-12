CREATE SCHEMA IF NOT EXISTS BL_DM;

SET SEARCH_PATH TO BL_DM;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_dm') THEN
        CREATE ROLE BL_DM;
    END IF;
END $$;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS  BL_dm.dm_Log_table (
    LogID bigserial PRIMARY KEY,
    LogDateTime timestamp,
    ProcedureName VARCHAR(255),
    NumRowsAffected bigINT,
    LogMessage text,
    Load_id int
);

CREATE or replace PROCEDURE BL_dm.dm_InsertLog(
    IN procedureName VARCHAR(255),
    IN numRowsAffected bigINT,
    IN logMessage text,
    IN i_load_id int
)
as $$
begin
    INSERT INTO BL_dm.dm_Log_table (LogDateTime, ProcedureName, NumRowsAffected, LogMessage, Load_id)
    VALUES (cast(NOW() as TIMESTAMP), procedureName, numRowsAffected, logMessage, i_load_id);
commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Create the Dim_Categories table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Categories (
    PK_Category_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Category text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);



-- Create the Dim_Subcategories table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Subcategories (
    PK_Subcategory_ID BIGSERIAL PRIMARY KEY,
    FK_Category_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Subcategory text  not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);



CREATE TABLE IF NOT EXISTS BL_DM.Dim_Dates (
    PK_Date_SURR_ID BIGSERIAL PRIMARY KEY,
    Full_Date date not null unique,
    Day int,
    Month int,
    Year int,
    Quarter int
);

-- Create the Dim_Sectors table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Sectors (
    PK_Sector_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Sector text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Segments table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Segments (
    PK_Segment_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Segment text   not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Markets table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Markets (
    PK_Market_ID BIGSERIAL PRIMARY KEY,
    Market text NOT NULL UNIQUE,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Orders table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Orders (
    PK_Order_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Order_Number text not null unique,
    Ship_Date DATE,
    Order_Priority TEXT,
    Ship_Mode TEXT
);

-- Create the Dim_Regions table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Regions (
    PK_Region_ID BIGSERIAL PRIMARY KEY,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Region text NOT NULL UNIQUE,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Countries table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Countries (
    PK_Country_ID BIGSERIAL PRIMARY KEY,
    FK_Region_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Country text not null unique,
    INSERT_DT DATE,
    UPDATE_DT DATE
);

-- Create the Dim_Addresses table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Addresses (
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

-- Create a table for dimension representing Products with slowly changing dimension type 2 (SCD2)
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Products_SCD2 (
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

-- Create a table for dimension representing Customers with slowly changing dimension type 2 (SCD2)
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Customers_SCD2 (
    PK_Customer_ID BIGSERIAL PRIMARY KEY,
    FK_Sector_ID BIGSERIAL,
    FK_Segment_ID BIGSERIAL,
    Source_system VARCHAR(255),
    Source_entity VARCHAR(255),
    Source_id VARCHAR(255),
    Customer_NR text,
    Tax_Number TEXT,
    Contact_person TEXT,
    Customer_Name TEXT,
    Email TEXT,
    Gender TEXT,
    Age_Group TEXT,
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE BOOLEAN
);

-- Create the Dim_Employees_SCD2 table if it does not exist
CREATE TABLE IF NOT EXISTS BL_DM.Dim_Employees_SCD2 (
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
CREATE TABLE IF NOT EXISTS BL_DM.FCT_ORDERS_DD (
    FK_Employee_ID BIGSERIAL,
    FK_Customer_ID BIGSERIAL,
    FK_Shipping_Address_ID BIGSERIAL,
    FK_Product_ID BIGSERIAL,
    FK_Order_ID BIGSERIAL,
    FK_Market_ID BIGSERIAL,
    FK_Date_ID BIGSERIAL,
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    Shipping_Cost FLOAT
);





grant execute on procedure BL_dm.dm_InsertLog to bl_cl;
/*grant execute on procedure dm_addresses to bl_cl;
grant execute on procedure dm_categories to bl_cl;
grant execute on procedure dm_countries to bl_cl;
grant execute on procedure dm_dates to bl_cl;
grant execute on procedure dm_employees to bl_cl;
grant execute on procedure dm_markets to bl_cl;
grant execute on procedure dm_orders to bl_cl;
grant execute on procedure dm_products to bl_cl;
grant execute on procedure dm_regions to bl_cl;
grant execute on procedure dm_sectors to bl_cl;
grant execute on procedure dm_segments to bl_cl;
grant execute on procedure dm_fact to bl_cl;
grant execute on procedure dm_subcategories to bl_cl;
grant execute on procedure run_dm to bl_cl;*/
grant select, update, insert on all tables in schema bl_dm to bl_cl;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA bl_dm TO bl_cl;

--------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------Usage of composite type -------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
/*CREATE type person_type AS (
    first_name text,
    last_name text
);*/

/*CREATE TABLE IF NOT EXISTS BL_DM.employees (
    employee_info person_type ,
    PRIMARY key (employee_info)
);


INSERT INTO BL_DM.employees (employee_info)
SELECT (e.employee_firstname, e.employee_lastname)::person_type -- Using composite type syntax
FROM bl_dm.dim_employees_scd2 e
WHERE e.IS_ACTIVE = true
AND e.employee_firstname <> 'NA'
AND e.employee_lastname <> 'NA'
ON CONFLICT (employee_info) DO NOTHING;
commit;

select * from BL_DM.employees e;*/


create materialized view if not exists yearly_sales_profit as
select  d."year", m.market, ROUND(sum(o.sales)::numeric, 2) AS total_sales, ROUND(sum(o.profit)::numeric, 2) AS total_profit
FROM BL_DM.FCT_ORDERS_DD o
LEFT JOIN bl_dm.Dim_Customers_SCD2 c ON o.FK_Customer_ID = c.pk_customer_id
LEFT JOIN bl_dm.Dim_Addresses a ON o.FK_Shipping_Address_ID = a.pk_address_id
LEFT JOIN bl_dm.Dim_Products_SCD2 p ON o.FK_Product_ID = p.pk_product_id
LEFT JOIN bl_dm.Dim_Orders od ON o.FK_Order_ID = od.pk_order_id
LEFT JOIN bl_dm.Dim_Markets m ON o.FK_Market_ID = m.pk_market_id
LEFT JOIN bl_dm.Dim_Dates d ON o.FK_Date_ID = d.pk_date_surr_id
LEFT JOIN bl_dm.dim_employees_scd2 des ON o.FK_Employee_ID = des.pk_employee_id
group by m.market, d."year"
order by "year", market;


create materialized view if not exists sales_employees_regions as
select  des.employee_firstname || ' ' || des.employee_lastname as "employee_name", coalesce(dr.region, 'NA') ,ROUND( sum(o.sales)::numeric, 2) AS total_sales
FROM BL_DM.FCT_ORDERS_DD o
LEFT JOIN bl_dm.Dim_Customers_SCD2 c ON o.FK_Customer_ID = c.pk_customer_id
LEFT JOIN bl_dm.Dim_Addresses a ON o.FK_Shipping_Address_ID = a.pk_address_id
LEFT JOIN bl_dm.Dim_Products_SCD2 p ON o.FK_Product_ID = p.pk_product_id
LEFT JOIN bl_dm.Dim_Orders od ON o.FK_Order_ID = od.pk_order_id
LEFT JOIN bl_dm.Dim_Markets m ON o.FK_Market_ID = m.pk_market_id
LEFT JOIN bl_dm.Dim_Dates d ON o.FK_Date_ID = d.pk_date_surr_id
LEFT JOIN bl_dm.dim_employees_scd2 des ON o.FK_Employee_ID = des.pk_employee_id
LEFT JOIN bl_dm.dim_countries dc  ON a.fk_country = dc.pk_country_id
LEFT JOIN bl_dm.dim_regions dr  on dc.pk_country_id  = dr.pk_region_id
group by des.employee_firstname || ' ' || des.employee_lastname , dr.region;
