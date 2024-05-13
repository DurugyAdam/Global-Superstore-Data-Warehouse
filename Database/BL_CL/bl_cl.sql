create schema if not exists BL_CL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_cl') THEN
        CREATE ROLE BL_CL;
    END IF;
END $$;
set SEARCH_PATH to BL_CL;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create table if not exists bl_cl.current_load_id(
 load_id int
 );

CREATE OR REPLACE FUNCTION bl_cl.setting_load_id()
RETURNS bigint AS $$
declare
    v_record_no int;
    v_load_id   int;
BEGIN
    -- Retrieve the maximum load_id or 0 if no rows exist
	select count(*) into v_record_no from bl_cl.current_load_id;
    if v_record_no = 0 then insert into bl_cl.current_load_id (load_id) values (0);
   else
    SELECT
     case when load_id is null then 0
     else COALESCE(load_id, 0) + 1 end INTO v_load_id FROM bl_cl.current_load_id;
    update bl_cl.current_load_id set load_id = v_load_id;
   end if;
    RETURN v_load_id;
END;
$$ LANGUAGE plpgsql;


grant insert, update, select on bl_cl.current_load_id to sa_superstore_consumer;
grant insert, update, select on bl_cl.current_load_id to sa_superstore_corporate;

grant execute on function bl_cl.setting_load_id() to sa_superstore_consumer;
grant execute on function bl_cl.setting_load_id() to sa_superstore_corporate;

/*The data from the external table is inserted into the src table and also the date of the insert and the load_id, that is the identifier of the pipeline*/
create or replace procedure bl_cl.scr_load ()
as $$
declare
   v_load_id int;
   check_load_id VARCHAR(255);
begin
check_load_id := null;
select load_id into v_load_id from bl_cl.current_load_id;
select distinct load_id into check_load_id from sa_SuperStore_Consumer.scr_Consumer sc where sc.load_id = v_load_id::VARCHAR(255);
if check_load_id is not null then
RAISE exception 'This data was already loaded';
else
insert into sa_SuperStore_Consumer.scr_Consumer select ec.*, to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS Insert_Date, v_load_id::VARCHAR(255) as Load_id from sa_SuperStore_Consumer.ext_Consumer ec;
commit;
end if;
select distinct load_id into check_load_id from sa_SuperStore_Corporate.scr_Corporate sc where sc.load_id = v_load_id::VARCHAR(255);

if check_load_id is not null then
RAISE exception 'This data was already loaded';
else
insert into sa_SuperStore_Corporate.scr_Corporate select ec.*, to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS Insert_Date, v_load_id::VARCHAR(255) as Load_id from sa_SuperStore_Corporate.ext_Corporate ec;
commit;
end if;
end;
$$
LANGUAGE plpgsql;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------Loading into source tables the content of external tables. The load_id is constant during the whole process, from source, bl_3nf till the end of bl_dm--------------------------------------------------------

select bl_cl.setting_load_id();
call bl_cl.scr_load ();


/*select * from bl_cl.current_load_id;
select * from sa_SuperStore_Consumer.scr_Consumer where load_id = '1';
select * from sa_SuperStore_Corporate.scr_Corporate where load_id = '1';*/

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


/*I created the map customer_consumer table to clean the customer data from the consumer source*/

CREATE TABLE if not exists bl_cl.map_customer_consumer (
    cleaned_customer_number TEXT,
    cleaned_customer_name TEXT,
    Gender TEXT,
    Age_Group text,
    Email text,
    CONSTRAINT unique_customer_Consumer UNIQUE (cleaned_customer_number, cleaned_customer_name)
);

create or replace procedure bl_cl.m_customer_consumer(i_load_id int)
as $$

begin
INSERT INTO bl_cl.map_customer_consumer (cleaned_customer_number, cleaned_customer_name, Gender, Age_Group, Email)
SELECT DISTINCT
    CASE
        WHEN POSITION('#' IN Customer_Nr) > 0
        THEN SUBSTRING(Customer_Nr FROM 1 FOR POSITION('#' IN Customer_Nr) - 1)
        ELSE Customer_Nr
    END AS cleaned_customer_number,
    CASE
        WHEN POSITION(' -' IN Customer_Name) > 0
        THEN SUBSTRING(Customer_Name FROM 1 FOR POSITION(' -' IN Customer_Name) - 1)
        ELSE Customer_Name
    END AS cleaned_customer_name,
    Gender,
    Age_Group,
    Email
FROM sa_SuperStore_Consumer.scr_Consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
ON CONFLICT (cleaned_customer_number, cleaned_customer_name) DO NOTHING;
commit;
end;
$$
LANGUAGE plpgsql;


/*I created the map customer_corporate table to clean the customer data from the corporate source*/

CREATE TABLE if not exists bl_cl.map_customer_corporate (
    cleaned_Customer_ID TEXT,
    cleaned_Contact_Person TEXT,
    Tax_Number TEXT,
    Sector text,
    segment text,
    Email text,
    CONSTRAINT unique_customer_Corporate UNIQUE (cleaned_Customer_ID, cleaned_Contact_Person)
);


create or replace procedure bl_cl.m_customer_corporate(i_load_id int)
as $$

begin

INSERT INTO bl_cl.map_customer_corporate (cleaned_Customer_ID, cleaned_Contact_Person, Tax_Number, Sector, Segment, Email)
SELECT DISTINCT
    CASE
        WHEN POSITION('#' IN Customer_ID) > 0
        THEN SUBSTRING(Customer_ID FROM 1 FOR POSITION('#' IN Customer_ID) - 1)
        ELSE Customer_ID
    END AS cleaned_customer_number,
    CASE
        WHEN POSITION(' -' IN Contact_Person) > 0
        THEN SUBSTRING(Contact_Person FROM 1 FOR POSITION(' -' IN Contact_Person) - 1)
        ELSE Contact_Person
    END AS cleaned_Contact_Person,
    Tax_Number,
    Sector,
    Segment,
    Email
FROM sa_SuperStore_Corporate.scr_Corporate sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
ON CONFLICT (cleaned_Customer_ID, cleaned_Contact_Person) DO NOTHING;

commit;
end;
$$
LANGUAGE plpgsql;

/*I created the map product to clean and deduplicate the product data from both sources*/

CREATE TABLE if not exists bl_cl.map_product (
    cleaned_product_number TEXT,
    Category_Name TEXT,
    Sub_Category_Name TEXT,
    Product_Name text,
    CONSTRAINT unique_product UNIQUE (cleaned_product_number)
);

create or replace procedure bl_cl.m_product_consumer(i_load_id int)
as $$

begin

INSERT INTO bl_cl.map_product (cleaned_product_number, Category_Name, Sub_Category_Name, Product_Name)
select
  mp.cleaned_product_number,
  mp.Category_Name,
  mp.Sub_Category_Name,
  mp.Product_Name
from
(SELECT DISTINCT
    CASE
        WHEN POSITION('#' IN Product_Nr) > 0
        THEN SUBSTRING(Product_Nr FROM 1 FOR POSITION('#' IN Product_Nr) - 1)
        ELSE Product_Nr
    END AS cleaned_product_number,
    Category_Name,
    Sub_Category_Name,
    Product_Name
FROM sa_SuperStore_Consumer.scr_Consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))) mp
ON CONFLICT (cleaned_product_number) DO NOTHING;
commit;
end;
$$
LANGUAGE plpgsql;


create or replace procedure bl_cl.m_product_corporate(i_load_id int)
as $$

begin

INSERT INTO bl_cl.map_product (cleaned_product_number, Category_Name, Sub_Category_Name, Product_Name)
select
  mp.cleaned_product_number,
  mp.Category_Name,
  mp.Sub_Category_Name,
  mp.Product_Name
from
(SELECT DISTINCT
    CASE
        WHEN POSITION('#' IN Product_ID) > 0
        THEN SUBSTRING(Product_ID FROM 1 FOR POSITION('#' IN Product_ID) - 1)
        ELSE Product_ID
    END AS cleaned_product_number,
    sc.Category as Category_Name,
    sc.Sub_Category as Sub_Category_Name,
    sc.Product as Product_Name
FROM sa_SuperStore_Corporate.scr_Corporate sc
where sc.load_id = cast(i_load_id as VARCHAR(255))) mp
ON CONFLICT (cleaned_product_number) DO NOTHING;
commit;
end;
$$
LANGUAGE plpgsql;


--------------Create table map_market and loading function-----
CREATE TABLE if not exists bl_cl.map_Market (
	PK_Market_ID bigserial PRIMARY KEY,
    Market TEXT,
    Source_system VARCHAR(255),
    Source_id VARCHAR(255)
);


CREATE OR REPLACE procedure bl_cl.m_MARKET(i_load_id int)
AS $$
DECLARE
    MARKET_cursor CURSOR for
select source_market.Market, source_market.Source_system, source_market.Source_id
from
(select distinct market as Market , 'Global_Superstore_Orders' as Source_system, market as Source_id
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct market as Market , 'Global_Superstore_Orders' as Source_system, market as Source_id
from sa_superstore_corporate.scr_corporate sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
) source_market
left join bl_cl.map_Market map_market
on source_market.market = map_market.market
where map_market.market is null;
rec RECORD;
BEGIN
    FOR rec IN MARKET_cursor LOOP
	    EXECUTE format('INSERT INTO map_Market (Market, Source_system, Source_id) VALUES (%L, %L, %L)', rec.Market, rec.Source_system, rec.Source_id);
	    RAISE NOTICE 'currently processing';
    END LOOP;
END;
$$
LANGUAGE plpgsql;



create or replace procedure bl_cl.bl_cl_load()
as $$
declare
   v_load_id int;
begin
select load_id into v_load_id from bl_cl.current_load_id;
call BL_CL.m_customer_consumer(v_load_id);
call BL_CL.m_customer_corporate(v_load_id);
call BL_CL.m_product_consumer(v_load_id);
call BL_CL.m_product_corporate(v_load_id);
call BL_CL.m_MARKET(v_load_id);
end;
$$
LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------Loading into BL_CL - data cleaning and deduplication-----------------------------------------------------------------------------------------------------------------------------------------

call bl_cl.bl_cl_load();


-----------------------------------------------------------------------------------------------------------------------------------------------------------
/*---------------------------------------------------------------------------------------------------------------------------------------------------------*/
-----------------IMPORTANT: Before running the following procedures, it must be run the bl_3nf and bl_dm , to create the tables in those schemas--------
-----------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------The following are the procedures for loading bl_3nf---------------------------------------------------------------------------------




create or replace procedure d_categories(i_load_id int)
as $load$
declare
    row_no bigint;

begin



select count(*) into row_no
from
(select distinct
sc.category_name  as Category
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct
sc2.category  AS Category
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) rn ;

if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';

else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Categories
INSERT INTO BL_3NF.Dim_Categories(Source_system, Source_entity, Source_id, Category, INSERT_DT, UPDATE_DT)
select coalesce(consumer.Source_system, corporate.Source_system) as Source_system,
coalesce(consumer.Source_entity, corporate.Source_entity) as Source_entity,
coalesce(consumer.Source_id, corporate.Source_id) as Source_id,
coalesce(consumer.Category, corporate.Category) as Category,
coalesce(consumer.INSERT_DT, corporate.INSERT_DT) as INSERT_DT,
coalesce(consumer.UPDATE_DT, corporate.UPDATE_DT) as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.category_name as Source_id,
sc.category_name  as Category,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))) consumer
full outer join
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' AS Source_entity,
sc2.category  AS Source_id,
sc2.category  AS Category,
CAST(NOW() AS DATE) AS INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') AS UPDATE_DT
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) corporate
ON consumer.Source_id = corporate.Source_id
ON CONFLICT (Category) DO NOTHING;

call bl_3nf.InsertLog( 'd_categories', row_no::bigint , 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_sectors(i_load_id int)
as $load$
declare
    row_no_corporate bigint;
begin

select count(distinct Sector) into row_no_corporate from sa_superstore_corporate.scr_corporate sc where sc.load_id = cast(i_load_id as VARCHAR(255));
if row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the corporate source.';
else
-- Insert new records from SRC_CONSUMER into Dim_Sectors
INSERT INTO BL_3NF.Dim_Sectors(Source_system, Source_entity, Source_id, Sector, INSERT_DT, UPDATE_DT)
select coalesce(corporate.Source_system) as Source_system,
coalesce(corporate.Source_entity) as Source_entity,
coalesce(corporate.Source_id, 'NA') as Source_id,
coalesce(corporate.Sector, 'NA') as Sector,
coalesce(corporate.INSERT_DT) as INSERT_DT,
coalesce(corporate.UPDATE_DT) as UPDATE_DT
FROM
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CORPORATE' as Source_entity,
sc2.Sector  as Source_id,
sc2.Sector  as Sector,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))
) corporate
ON CONFLICT (Sector) DO NOTHING;
call bl_3nf.InsertLog( 'd_sectors', row_no_corporate::bigint , 'Procedure run succesfully', i_load_id);
COMMIT;
end if;
end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_markets(i_load_id int)
as $load$
declare
	row_no bigint;

begin

select count(*) into row_no
from
(select distinct
sc.market  as Market
from sa_superstore_consumer.scr_consumer sc where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct
sc2.market  AS Market
FROM sa_superstore_corporate.scr_corporate sc2 where sc2.load_id = cast(i_load_id as VARCHAR(255))) ;

if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Markets
INSERT INTO BL_3NF.Dim_Markets (Market, Source_system, Source_entity, Source_id, INSERT_DT, UPDATE_DT)
-- Records from SCR_CONSUMER
select distinct m.market as Market , 'GLOBAL_SUPERSTORE' as Source_system, 'map_Market' as Source_entity, m.market as Source_id,
cast('2024-01-01'as date) AS INSERT_DT, cast('2024-01-01'as date)  AS UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
left join bl_cl.map_market m on sc.market = m.source_id
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
-- Records from SCR_CORPORATE
select distinct m.market as Market , 'GLOBAL_SUPERSTORE' as Source_system, 'map_Market' as Source_entity, m.market as Source_id,
cast('2024-01-01'as date) AS INSERT_DT, cast('2024-01-01'as date)  AS UPDATE_DT
from sa_superstore_corporate.scr_corporate sc2
left join bl_cl.map_market m on sc2.market = m.source_id
where sc2.load_id = cast(i_load_id as VARCHAR(255))
on conflict (Market) do update set UPDATE_DT =  cast(now() AS date);
call bl_3nf.InsertLog( 'd_markets', row_no::bigint ,  'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_segment(i_load_id int)
as $load$
declare
    row_no_consumer bigint;
    row_no_corporate bigint;
    begin


select count(distinct segment) into row_no_consumer from sa_superstore_consumer.scr_consumer sc where sc.load_id = cast(i_load_id as VARCHAR(255));
select count(distinct segment) into row_no_corporate from sa_superstore_corporate.scr_corporate sc where sc.load_id = cast(i_load_id as VARCHAR(255));
if row_no_consumer + row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SRC_CONSUMER and SRC_CORPORATE into Dim_Segments
INSERT INTO BL_3NF.Dim_Segments(Source_system, Source_entity, Source_id, Segment, INSERT_DT, UPDATE_DT)
-- Records from SRC_CONSUMER
select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.segment  as Source_id,
sc.segment  as Segment,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union all
-- Records from SRC_CORPORATE
select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CORPORATE' as Source_entity,
sc2.segment  as Source_id,
sc2.segment  as Segment,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))
ON CONFLICT (Segment) DO NOTHING;
call bl_3nf.InsertLog( 'd_segment', row_no_consumer::bigint+row_no_corporate::bigint , 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_regions(i_load_id int)
as $load$
declare
    row_no_corporate bigint;

begin

select count(distinct region) into row_no_corporate from sa_superstore_corporate.scr_corporate sc where sc.load_id = cast(i_load_id as VARCHAR(255));
if row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the corporate source.';
else
-- Insert new records from SCR_CORPORATE into Dim_Regions
INSERT INTO BL_3NF.Dim_Regions(Source_system, Source_entity, Source_id, Region, INSERT_DT, UPDATE_DT)
select corporate.Source_system as Source_system,
corporate.Source_entity as Source_entity,
corporate.Source_id as Source_id,
corporate.Region as Region,
corporate.INSERT_DT as INSERT_DT,
corporate.UPDATE_DT as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CORPORATE' as Source_entity,
sc2.Region  as Source_id,
sc2.Region  as Region,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) corporate
ON CONFLICT (Region) DO NOTHING;
call bl_3nf.InsertLog( 'd_regions', row_no_corporate::bigint , 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_countries(i_load_id int)
as $load$
declare
row_no bigint;

begin

select count(*) into row_no
from
(select distinct
sc.country  as country
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct
sc2.country  AS country
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) ;

if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Countries

INSERT INTO BL_3NF.Dim_Countries(FK_Region_ID, Source_system, Source_entity, Source_id, Country, INSERT_DT, UPDATE_DT)
select
reg.PK_Region_ID as FK_Region_ID,
corporate.Source_system as Source_system,
corporate.Source_entity as Source_entity,
corporate.Source_id as Source_id,
corporate.Country as Country,
corporate.INSERT_DT as INSERT_DT,
corporate.UPDATE_DT as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.Country  as Source_id,
sc.Country  as Country,
sc.region  as Region,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_corporate.scr_corporate sc
where Country is not null and sc.load_id = cast(i_load_id as VARCHAR(255))) corporate
left join bl_3nf.dim_regions  reg
on reg.region = coalesce(corporate.region, 'NA')
ON CONFLICT (Country) DO NOTHING;
COMMIT;
end if;

if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
INSERT INTO BL_3NF.Dim_Countries(FK_Region_ID, Source_system, Source_entity, Source_id, Country, INSERT_DT, UPDATE_DT)
select
reg.PK_Region_ID as FK_Region_ID,
consumer.Source_system as Source_system,
consumer.Source_entity as Source_entity,
consumer.Source_id as Source_id,
consumer.Country as Country,
consumer.INSERT_DT as INSERT_DT,
consumer.UPDATE_DT as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.Country  as Source_id,
sc.Country  as Country,
--sc.region  as Region,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
where Country is not null and sc.load_id = cast(i_load_id as VARCHAR(255))) consumer
left join bl_3nf.dim_regions  reg
on reg.region = 'NA'
ON CONFLICT (Country) DO NOTHING;
COMMIT;

call bl_3nf.InsertLog( 'd_countries', row_no::bigint  , 'Procedure run succesfully', i_load_id);
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_orders(i_load_id int)
as $load$
declare row_no_consumer bigint;
    row_no_corporate bigint;

begin
select count(distinct order_number) into row_no_consumer from sa_superstore_consumer.scr_consumer sc where sc.load_id = cast(i_load_id as VARCHAR(255));
select count(distinct order_id) into row_no_corporate from sa_superstore_corporate.scr_corporate sc where sc.load_id = cast(i_load_id as VARCHAR(255));
if row_no_consumer + row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Orders
INSERT INTO BL_3NF.Dim_Orders(Source_system, Source_entity, Source_id, Order_Number, Ship_Date, Order_Priority, Ship_Mode)
-- Records from SCR_CONSUMER
select distinct 'Global_Superstore_Orders' as Source_system,
'SRC_Consumer' as Source_entity,
sc.order_number as Source_id,
sc.order_number as Order_Number,
to_date(sc.ship_date, 'DD-MM-YYYY') as Ship_Date,
sc.order_priority as Order_Priority,
sc.ship_mode as Ship_Mode
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
-- Records from SCR_CORPORATE
select distinct 'Global_Superstore_Orders' as Source_system,
'SRC_Corporate' as Source_entity,
sc2.order_id  as Source_id,
sc2.order_id  as Order_Number,
to_date(sc2.Shipping, 'DD-MM-YYYY') as Ship_Date, sc2.order_priority as Order_Priority, sc2.ship_mode as Ship_Mode
from sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))
on conflict (Order_Number) do nothing;
call bl_3nf.InsertLog( 'd_orders', row_no_consumer::bigint+row_no_corporate::bigint , 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure d_addresses(i_load_id int)
as $load$
declare row_no_consumer bigint;
    row_no_corporate bigint;
begin

select count(distinct Address) into row_no_consumer from sa_superstore_consumer.scr_consumer sc where sc.load_id = cast(i_load_id as VARCHAR(255));
select count(distinct Shipping_address ) into row_no_corporate from sa_superstore_corporate.scr_corporate sc where sc.load_id = cast(i_load_id as VARCHAR(255));
if row_no_consumer + row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Addresses
INSERT INTO BL_3NF.Dim_Addresses(FK_Country, Source_system, Source_entity, Source_id, Shipping_Address, City, Postal_Code, INSERT_DT, UPDATE_DT)
select
Dim_Countries.PK_Country_ID as FK_Country_ID,
address.Source_system as Source_system,
address.Source_entity as Source_entity,
coalesce(address.Source_id,'NA') as Source_id,
coalesce(address.Shipping_Address, 'NA') as Shipping_Address,
address.City as City,
address.Postal_Code as Postal_Code,
address.INSERT_DT as INSERT_DT,
address.UPDATE_DT as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.address  as Source_id,
sc.address  as Shipping_Address,
sc.City  as City,
sc.country as country,
sc.Postal_Code  as Postal_Code,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
where sc.address is not null and sc.load_id = cast(i_load_id as VARCHAR(255))
) address
left join bl_3nf.Dim_Countries on Dim_Countries.country = coalesce(address.country, 'NA')
ON CONFLICT (Shipping_Address) DO NOTHING;
COMMIT;

INSERT INTO BL_3NF.Dim_Addresses(FK_Country, Source_system, Source_entity, Source_id, Shipping_Address, City, Postal_Code, INSERT_DT, UPDATE_DT)
select
Dim_Countries.PK_Country_ID as FK_Region_ID,
address.Source_system as Source_system,
address.Source_entity as Source_entity,
coalesce(address.Source_id,'NA') as Source_id,
coalesce(address.Shipping_Address, 'NA') as Shipping_Address,
address.City as City,
address.Postal_Code as Postal_Code,
address.INSERT_DT as INSERT_DT,
address.UPDATE_DT as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CORPORATE' as Source_entity,
sc.shipping_address  as Source_id,
sc.shipping_address  as Shipping_Address,
sc.City  as City,
sc.Postal_Code  as Postal_Code,
sc.country as country,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_corporate.scr_corporate  sc
where sc.shipping_address is not null and sc.load_id = cast(i_load_id as VARCHAR(255))
) address
left join bl_3nf.Dim_Countries on Dim_Countries.country = coalesce(address.country, 'NA')
ON CONFLICT (Shipping_Address) DO NOTHING;
call bl_3nf.InsertLog( 'd_addresses', row_no_consumer::bigint+row_no_corporate::bigint, 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure d_subcategories(i_load_id int)
as $load$
declare row_no bigint;
begin

select count(*) into row_no
from
(select distinct
sc.sub_category_name  as Subcategory
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct
sc2.sub_category AS Subcategory
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) ;
if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
-- Insert new records from SCR_CONSUMER and SCR_CORPORATE into Dim_Subcategories
INSERT INTO BL_3NF.Dim_Subcategories(FK_Category_ID,Source_system , Source_entity, Source_id, Subcategory, INSERT_DT, UPDATE_DT)
select
Dim_Categories.PK_Category_ID as FK_Category_ID,
subcategory.Source_system as Source_system,
subcategory.Source_entity as Source_entity,
subcategory.Source_id as Source_id,
subcategory.Subcategory as Subcategory,
subcategory.INSERT_DT as INSERT_DT,
subcategory.UPDATE_DT as UPDATE_DT
from
(select coalesce(consumer.Source_system, corporate.Source_system) as Source_system,
coalesce(consumer.Source_entity, corporate.Source_entity) as Source_entity,
coalesce(consumer.Source_id, corporate.Source_id) as Source_id,
coalesce(consumer.Subcategory, corporate.Subcategory) as Subcategory,
coalesce(consumer.category, corporate.category) as category,
coalesce(consumer.INSERT_DT, corporate.INSERT_DT) as INSERT_DT,
coalesce(consumer.UPDATE_DT, corporate.UPDATE_DT) as UPDATE_DT
from
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc.sub_category_name  as Source_id,
sc.sub_category_name  as Subcategory,
sc.category_name as category ,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))) consumer
full outer join
(select distinct 'GLOBAL_SUPERSTORE' as source_system,
'SRC_CONSUMER' as Source_entity,
sc2.sub_category  as Source_id,
sc2.sub_category  as Subcategory,
sc2.category as category ,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) corporate
on consumer.Source_id = corporate.Source_id) subcategory
left join bl_3nf.Dim_Categories on Dim_Categories.category = subcategory.category
ON CONFLICT (Subcategory) DO NOTHING;
call bl_3nf.InsertLog( 'd_subcategories', row_no::bigint, 'Procedure run succesfully', i_load_id);
COMMIT;
end if;

end;
$load$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure d_products(i_load_id int)
as $load$
declare row_no bigint;

begin
select count(distinct prod.cleaned_product_number) into row_no
from BL_CL.map_product  prod ;
if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
merge INTO bl_3nf.Dim_Products_SCD2 nf_product
using
(
select distinct
     ds.pk_subcategory_id  as FK_Subcategory_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_PRODUCT' AS Source_entity,
    coalesce(prod.cleaned_product_number  , 'NA') AS Source_id,
    coalesce(prod.cleaned_product_number , 'NA') as Product_NR,
    prod.product_name  as Product_name

FROM
    BL_CL.map_product  prod
 left join bl_3nf.dim_subcategories ds on ds.subcategory = prod.sub_category_name
   ) src_product
 on  src_product.Product_NR = nf_product.Product_NR
 and nf_product.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when matched
and nf_product.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
and ( nf_product.product_name  <> src_product.product_name)
 then update set END_DT = CAST(NOW() AS DATE),
                  IS_ACTIVE = false;

merge INTO bl_3nf.Dim_Products_SCD2 nf_product
using
(
select distinct
     ds.pk_subcategory_id  as FK_Subcategory_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_PRODUCT' AS Source_entity,
    coalesce(prod.cleaned_product_number  , 'NA') AS Source_id,
    coalesce(prod.cleaned_product_number , 'NA') as Product_NR,
    prod.product_name  as Product_name

FROM
    BL_CL.map_product  prod
 left join bl_3nf.dim_subcategories ds on ds.subcategory = prod.sub_category_name
   ) src_product
 on  src_product.Product_NR = nf_product.Product_NR
 and nf_product.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when not matched then
  insert (FK_Subcategory_ID, Source_system, Source_entity, Source_id, Product_NR,Product_name, START_DT,END_DT,IS_ACTIVE)
  values
  ( src_product.FK_Subcategory_ID,
  	src_product.source_system ,
    src_product.Source_entity ,
    src_product.Source_id,
    src_product.Product_NR,
    src_product.Product_name ,

	CAST(NOW() AS DATE) ,
	DATE '9999-12-31' ,
	TRUE);
call bl_3nf.InsertLog( 'd_products', row_no::bigint, 'Procedure run succesfully', i_load_id);
commit;

end if;

end;
$load$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_customers(i_load_id int)
as $load$
declare row_no_consumer bigint;
    row_no_corporate bigint;
begin

select count( distinct cleaned_customer_number) into row_no_consumer from BL_CL.map_customer_consumer;
select count(distinct cleaned_Customer_ID) into row_no_corporate from BL_CL.map_customer_corporate;
if row_no_consumer + row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
merge INTO bl_3nf.Dim_Customers_SCD2 nf_customer
using
(
select distinct
    -1 as FK_Sector_ID,
     seg.pk_segment_id as FK_Segment_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_CONSUMER' AS Source_entity,
    coalesce(cons.cleaned_customer_number , 'NA') AS Source_id,
    coalesce(cons.cleaned_customer_number, 'NA') as Customer_NR,
  /*  Tax_Number,
    Contact_person,*/
    cons.cleaned_customer_name  as Customer_Name,
    Email ,
    Gender ,
    Age_Group
FROM
    BL_CL.map_customer_consumer cons
 left join bl_3nf.dim_segments  seg on upper( seg.segment ) =  'CONSUMER'
   ) src_customer
 on nf_customer.customer_nr =  src_customer.customer_nr
 and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when matched and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd') and
(/*src_customer.Tax_Number <> nf_customer.Tax_Number
or src_customer.Contact_person <> nf_customer.Contact_person*/
   src_customer.Customer_Name <> nf_customer.Customer_Name
or src_customer.Email <> nf_customer.Email
or src_customer.Gender <> nf_customer.Gender
or src_customer.Age_Group <> nf_customer.Age_Group)
 then update set END_DT = CAST(NOW() AS DATE),
                  IS_ACTIVE = false;

merge INTO bl_3nf.Dim_Customers_SCD2 nf_customer
using
(
select distinct
    sec.pk_sector_id  as FK_Sector_ID,
     seg.pk_segment_id as FK_Segment_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_CONSUMER' AS Source_entity,
    coalesce(corp.cleaned_customer_id  , 'NA') AS Source_id,
    coalesce(corp.cleaned_customer_id, 'NA') as Customer_NR,
    Tax_Number,
    corp.cleaned_contact_person as Contact_person,
    'NA'  as Customer_Name,
    Email
FROM
    BL_CL.map_customer_corporate corp
LEFT JOIN bl_3nf.dim_segments seg ON UPPER(seg.segment) = UPPER(corp.segment)
left join bl_3nf.dim_sectors SEC on SEC.sector = corp.sector
   ) src_customer
 on nf_customer.customer_nr =  src_customer.customer_nr
 and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when matched and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd') and
(src_customer.Tax_Number <> nf_customer.Tax_Number
or src_customer.Contact_person <> nf_customer.Contact_person
   --src_customer. <> nf_customer.Customer_Name
or src_customer.Email <> nf_customer.Email)
 then update set END_DT = CAST(NOW() AS DATE),
                  IS_ACTIVE = false;

merge INTO bl_3nf.Dim_Customers_SCD2 nf_customer
using
(
select distinct
    -1 as FK_Sector_ID,
     seg.pk_segment_id as FK_Segment_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_CONSUMER' AS Source_entity,
    coalesce(cons.cleaned_customer_number , 'NA') AS Source_id,
    coalesce(cons.cleaned_customer_number, 'NA') as Customer_NR,
  /*  Tax_Number,
    Contact_person,*/
    cons.cleaned_customer_name  as Customer_Name,
    Email ,
    Gender ,
    Age_Group
FROM
    BL_CL.map_customer_consumer cons
 left join bl_3nf.dim_segments  seg on upper( seg.segment ) =  'CONSUMER'
   ) src_customer
 on nf_customer.customer_nr =  src_customer.customer_nr
 and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when not matched then
  insert (FK_Sector_ID, FK_Segment_ID, Source_system, Source_entity, Source_id, Customer_NR,Tax_Number, Contact_person, Customer_Name,Email,Gender,Age_Group,START_DT,END_DT,IS_ACTIVE)
  values
  ( src_customer.FK_Sector_ID,
  	src_customer.FK_Segment_ID,
  	src_customer.source_system ,
    src_customer.Source_entity ,
    src_customer.Source_id,
    src_customer.Customer_NR,
	'NA',
    'NA',
    src_customer.Customer_Name ,
    src_customer.Email ,
    src_customer.Gender ,
    src_customer.Age_Group ,
	CAST(NOW() AS DATE) ,
	DATE '9999-12-31' ,
	TRUE)    ;

merge INTO bl_3nf.Dim_Customers_SCD2 nf_customer
using
(
select distinct
    sec.pk_sector_id  as FK_Sector_ID,
     seg.pk_segment_id as FK_Segment_ID,
    'GLOBAL_SUPERSTORE' AS Source_system,
    'SRC_CONSUMER' AS Source_entity,
    coalesce(corp.cleaned_customer_id  , 'NA') AS Source_id,
    coalesce(corp.cleaned_customer_id, 'NA') as Customer_NR,
    Tax_Number,
    corp.cleaned_contact_person as Contact_person,
    'NA'  as Customer_Name,
    Email
FROM
    BL_CL.map_customer_corporate corp
LEFT JOIN bl_3nf.dim_segments seg ON UPPER(seg.segment) = UPPER(corp.segment)
left join bl_3nf.dim_sectors SEC on SEC.sector = corp.sector
   ) src_customer
 on nf_customer.customer_nr =  src_customer.customer_nr
 and nf_customer.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when not matched then
  insert (FK_Sector_ID, FK_Segment_ID, Source_system, Source_entity, Source_id, Customer_NR,Tax_Number, Contact_person, Customer_Name,Email,Gender,Age_Group,START_DT,END_DT,IS_ACTIVE)
  values
  ( src_customer.FK_Sector_ID,
  	src_customer.FK_Segment_ID,
  	src_customer.source_system ,
    src_customer.Source_entity ,
    src_customer.Source_id,
    src_customer.Customer_NR,
	Tax_Number,
    Contact_person,
    src_customer.Customer_Name ,
    src_customer.Email ,
    'NA' ,
    'NA' ,
	CAST(NOW() AS DATE) ,
	DATE '9999-12-31' ,
	TRUE);
call bl_3nf.InsertLog( 'd_customers', row_no_consumer::bigint+row_no_corporate::bigint, 'Procedure run succesfully', i_load_id);
commit;

end if;

end;
$load$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure d_employees(i_load_id int)
as $load$
declare row_no bigint;
begin


select count(*) into row_no
from
(select distinct
sc.employee_NR as Employee_NR
from sa_superstore_consumer.scr_consumer sc
where sc.load_id = cast(i_load_id as VARCHAR(255))
union
select distinct
sc2.employee_id  AS Employee_NR
FROM sa_superstore_corporate.scr_corporate sc2
where sc2.load_id = cast(i_load_id as VARCHAR(255))) ;
if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
merge INTO bl_3nf.Dim_Employees_SCD2 nf_employee
using
(select distinct
    'GLOBAL_SUPERSTORE' AS Source_system,
--    'SRC_CORPORATE' AS Source_entity,
    coalesce(Employee_ID, 'NA') AS Source_id,
    coalesce(Employee_ID, 'NA') AS Employee_NR,
    SPLIT_PART(Employee_Name, ' ', 1) AS Employee_FirstName,
    SPLIT_PART(Employee_Name, ' ', 2) AS Employee_LastName
FROM
    sa_superstore_corporate.scr_Corporate sc1
    where sc1.load_id = cast(i_load_id as VARCHAR(255))
UNION
select distinct
    'GLOBAL_SUPERSTORE' AS Source_system,
--    'SRC_CONSUMER' AS Source_entity,
    coalesce(Employee_NR, 'NA') AS Source_id,
    coalesce(Employee_NR, 'NA') as Employee_NR,
    Employee_FirstName,
    Employee_LastName
FROM
    sa_superstore_consumer.scr_Consumer sc
    where sc.load_id = cast(i_load_id as VARCHAR(255))
     )  src_employee
    on nf_employee.employee_nr = src_employee.employee_nr
    and nf_employee.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when matched and nf_employee.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd') and (src_employee.Employee_FirstName <> nf_employee.Employee_FirstName or src_employee.Employee_LastName <> nf_employee.Employee_LastName)
 then update set  END_DT = CAST(NOW() AS DATE),
                  IS_ACTIVE = false;

end if;
if row_no = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else
merge INTO bl_3nf.Dim_Employees_SCD2 nf_employee
using
(select distinct
    'GLOBAL_SUPERSTORE' AS Source_system,
--    'SRC_CORPORATE' AS Source_entity,
    coalesce(Employee_ID, 'NA') AS Source_id,
    coalesce(Employee_ID, 'NA') AS Employee_NR,
    SPLIT_PART(Employee_Name, ' ', 1) AS Employee_FirstName,
    SPLIT_PART(Employee_Name, ' ', 2) AS Employee_LastName
FROM
    sa_superstore_corporate.scr_Corporate sc1
    where sc1.load_id = cast(i_load_id as VARCHAR(255))
UNION
select distinct
    'GLOBAL_SUPERSTORE' AS Source_system,
--    'SRC_CONSUMER' AS Source_entity,
    coalesce(Employee_NR, 'NA') AS Source_id,
    coalesce(Employee_NR, 'NA') as Employee_NR,
    Employee_FirstName,
    Employee_LastName
FROM
    sa_superstore_consumer.scr_Consumer sc2
    where sc2.load_id = cast(i_load_id as VARCHAR(255)))  src_employee
    on nf_employee.employee_nr = src_employee.employee_nr
    and nf_employee.END_DT = TO_DATE('9999-12-31', 'yyyy-mm-dd')
when not matched then
  insert ( Source_system, Source_entity, Source_id, Employee_NR,Employee_FirstName,Employee_LastName,START_DT,END_DT,IS_ACTIVE)
  values
  ( src_employee.source_system ,
    'SRC_EMPLOYEE' ,
    src_employee.Source_id,
	src_employee.Employee_NR ,
	src_employee.Employee_FirstName ,
	src_employee.Employee_LastName,
	CAST(NOW() AS DATE) ,
	DATE '9999-12-31' ,
	TRUE);
call bl_3nf.InsertLog( 'd_employees', row_no::bigint, 'Procedure run succesfully', i_load_id);
commit;

end if;

end;
$load$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Create partitions for the master table: BL_3NF.FCT_ORDERS_DD based on the Order_Date

CREATE OR REPLACE FUNCTION bl_cl.create_partition_for_date(date) RETURNS VOID AS $$
DECLARE
    partition_date ALIAS FOR $1;
    partition_name TEXT;
BEGIN
    partition_name := 'fct_orders_dd_' || EXTRACT(YEAR FROM partition_date);
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS BL_3NF.%I (
            CHECK (order_date >= DATE %L AND order_date < DATE %L)
        ) INHERITS (BL_3NF.fct_orders_dd);', partition_name, partition_date, (partition_date + INTERVAL '1 year'));

    EXECUTE format('
        CREATE INDEX ON BL_3NF.%I (order_date);', partition_name);

    RAISE NOTICE 'Partition created: %', partition_name;
END;
$$ LANGUAGE plpgsql;

-- Call the function to create partitions dynamically
create or replace procedure bl_cl.f_fact_partitions(i_load_id int)
as $$
DECLARE
    end_date DATE;
    cur_date DATE;-- := start_date;
begin
	select min(min_order_date) into cur_date from
	(select min(TO_DATE(order_date, 'DD-MM-YYYY')) as min_order_date from sa_superstore_consumer.scr_consumer where load_id = cast(i_load_id as VARCHAR(255))
	union
	select  min(TO_DATE(order_date, 'DD-MM-YYYY')) as min_order_date from sa_superstore_corporate.scr_corporate where load_id = cast(i_load_id as VARCHAR(255))) dt;

	select max(max_order_date) into end_date from
	(select max(TO_DATE(order_date, 'DD-MM-YYYY')) as max_order_date from sa_superstore_consumer.scr_consumer where load_id = cast(i_load_id as VARCHAR(255))
	union
	select  max(TO_DATE(order_date, 'DD-MM-YYYY')) as max_order_date from sa_superstore_corporate.scr_corporate where load_id = cast(i_load_id as VARCHAR(255))) dt;

     WHILE cur_date < end_date LOOP
        PERFORM bl_cl.create_partition_for_date(cur_date);
        cur_date := cur_date + INTERVAL '1 year';
    END LOOP;
END;
$$ language plpgsql;


create or replace procedure BL_cl.f_orders(i_load_id int)
as $load$
declare  row_no_consumer bigint;
    row_no_corporate bigint;

begin
select count(*) into row_no_consumer from sa_superstore_consumer.scr_consumer where load_id = cast(i_load_id as VARCHAR(255));
select count(*) into row_no_corporate from sa_superstore_corporate.scr_corporate where load_id = cast(i_load_id as VARCHAR(255));
if row_no_consumer + row_no_corporate = 0 then
  RAISE EXCEPTION 'There is no data in the source.';
else

INSERT INTO BL_3NF.FCT_ORDERS_DD(
	FK_Employee_ID ,
    FK_Customer_ID ,
    FK_Shipping_Address_ID ,
    FK_Product_ID ,
    FK_Order_ID ,
    FK_Market_ID ,
    Order_Date ,
    Sales ,
    Quantity ,
    Discount ,
    Profit ,
    Shipping_Cost,
    Load_id)
select
emp.pk_employee_id as FK_Employee_ID ,
cust.pk_customer_id as FK_Customer_ID ,
addr.pk_address_id as FK_Shipping_Address_ID ,
prod.pk_product_id as FK_Product_ID ,
ord.pk_order_id  as FK_Order_ID ,
mark.pk_market_id as FK_Market_ID ,
consumer.order_date as Order_Date,
CAST(consumer.sales as float) as Sales,
CAST(consumer.Quantity as int) as Quantity,
CAST(consumer.Discount as float) as Discount,
CAST(consumer.Profit as float) as Profit,
CAST(consumer.Shipping_Cost as float) as Shipping_Cost,
i_load_id as Load_id
from
(select
sc1.employee_nr ,
CASE
        WHEN POSITION('#' IN sc1.Customer_Nr) > 0
        THEN SUBSTRING(sc1.Customer_Nr FROM 1 FOR POSITION('#' IN sc1.Customer_Nr) - 1)
        ELSE sc1.Customer_Nr
    END AS cleaned_customer_number,
sc1.address as address,
CASE
        WHEN POSITION('#' IN Product_Nr) > 0
        THEN SUBSTRING(Product_Nr FROM 1 FOR POSITION('#' IN Product_Nr) - 1)
        ELSE Product_Nr
end cleaned_product_number,
sc1.order_number ,
sc1.market ,
to_date(sc1.order_date , 'DD-MM-YYYY') as order_date,
sc1.Sales,
sc1.Quantity,
sc1.Discount ,
sc1.Profit ,
sc1.Shipping_Cost
from sa_superstore_consumer.scr_consumer sc1
where sc1.load_id = cast(i_load_id as VARCHAR(255))
) consumer
left join bl_3nf.dim_employees_scd2  emp on emp.employee_nr = consumer.employee_nr
and emp.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and emp.is_active = true
left join bl_3nf.dim_customers_scd2 cust on cust.customer_nr = consumer.cleaned_customer_number
and cust.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and cust.is_active = true
left join bl_3nf.dim_addresses addr on addr.shipping_address = coalesce(consumer.address, 'NA')
and addr.update_dt  = to_date('9999-12-31', 'YYYY-MM-DD')
left join bl_3nf.dim_products_scd2  prod on prod.product_nr = consumer.cleaned_product_number
and prod.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and prod.is_active = true
left join bl_3nf.dim_orders ord on ord.order_number = consumer.order_number
left join bl_3nf.dim_markets  mark on mark.market = consumer.market
ON CONFLICT (FK_Employee_ID, FK_Customer_ID, FK_Shipping_Address_ID, FK_Product_ID, FK_Order_ID, FK_Market_ID) DO NOTHING;
commit;



INSERT INTO BL_3NF.FCT_ORDERS_DD(
	FK_Employee_ID ,
    FK_Customer_ID ,
    FK_Shipping_Address_ID ,
    FK_Product_ID ,
    FK_Order_ID ,
    FK_Market_ID ,
    Order_Date ,
    Sales ,
    Quantity ,
    Discount ,
    Profit ,
    Shipping_Cost,
    Load_id)
select
emp.pk_employee_id as FK_Employee_ID ,
cust.pk_customer_id as FK_Customer_ID ,
addr.pk_address_id as FK_Shipping_Address_ID ,
prod.pk_product_id as FK_Product_ID ,
ord.pk_order_id  as FK_Order_ID ,
mark.pk_market_id as FK_Market_ID ,
corporate.order_date as Order_Date,
CAST(corporate.sales as float) as Sales,
CAST(corporate.Quantity as int) as Quantity,
CAST(corporate.Discount as float) as Discount,
CAST(corporate.Profit as float) as Profit,
CAST(corporate.Shipping_Cost as float) as Shipping_Cost,
i_load_id as Load_id
from
(select
sc1.employee_id,
CASE
        WHEN POSITION('#' IN sc1.customer_id) > 0
        THEN SUBSTRING(sc1.customer_id  FROM 1 FOR POSITION('#' IN sc1.customer_id) - 1)
        ELSE sc1.customer_id
    END AS cleaned_customer_number,
sc1.shipping_address  as address,
CASE
        WHEN POSITION('#' IN sc1.product_id) > 0
        THEN SUBSTRING(sc1.product_id FROM 1 FOR POSITION('#' IN product_id) - 1)
        ELSE product_id
end cleaned_product_number,
sc1.order_id  ,
sc1.market ,
to_date(sc1.order_date , 'DD-MM-YYYY') as order_date,
sc1.Sales,
sc1.Quantity,
sc1.Discount ,
sc1.Profit ,
sc1.Shipping_Cost
from sa_superstore_corporate.scr_corporate  sc1
where sc1.load_id = cast(i_load_id as VARCHAR(255))
) corporate
left join bl_3nf.dim_employees_scd2  emp on emp.employee_nr = corporate.employee_id
and emp.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and emp.is_active = true
left join bl_3nf.dim_customers_scd2 cust on cust.customer_nr  = corporate.cleaned_customer_number
and cust.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and cust.is_active = true
left join bl_3nf.dim_addresses addr on addr.shipping_address = coalesce(corporate.address, 'NA')
and addr.update_dt  = to_date('9999-12-31', 'YYYY-MM-DD')
left join bl_3nf.dim_products_scd2  prod on prod.product_nr  = corporate.cleaned_product_number
and prod.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
and prod.is_active = true
left join bl_3nf.dim_orders ord on ord.order_number = corporate.order_id
left join bl_3nf.dim_markets  mark on mark.market = corporate.market
ON CONFLICT (FK_Employee_ID, FK_Customer_ID, FK_Shipping_Address_ID, FK_Product_ID, FK_Order_ID, FK_Market_ID) DO NOTHING;

commit;
call bl_3nf.InsertLog( 'f_orders', row_no_consumer::bigint, 'Number of consumer rows inserted', i_load_id);
call bl_3nf.InsertLog( 'f_orders', row_no_corporate::bigint, 'Number of corporate rows inserted', i_load_id);

end if;
end;
$load$ language plpgsql;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure run_3nf()
as $$
declare
   v_load_id int;
begin
select load_id into v_load_id from bl_cl.current_load_id;
call bl_cl.d_categories(v_load_id);
call bl_cl.d_sectors(v_load_id);
call bl_cl.d_segment(v_load_id);
call bl_cl.d_markets(v_load_id);
call bl_cl.d_regions(v_load_id);
call bl_cl.d_countries(v_load_id);
call bl_cl.d_orders(v_load_id);
call bl_cl.d_addresses(v_load_id);
call bl_cl.d_subcategories(v_load_id);
call bl_cl.d_products(v_load_id);
call bl_cl.d_customers(v_load_id);
call bl_cl.d_employees(v_load_id);
call bl_cl.f_fact_partitions(v_load_id);
call bl_cl.f_orders(v_load_id);
end;
$$ language plpgsql;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------end of procedures for loading bl_3nf
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
---------Loading bl_3nf layer-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select * from bl_cl.current_load_id;

call bl_cl.run_3nf();
select * from BL_3NF.Log_table l;


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------The following are the procedures for loading bl_dm---------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



create or replace procedure dm_categories(i_load_id int)
as $$
declare
    row_no bigint;
begin


select count(*) into row_no
from
bl_3nf.dim_categories  ds
left join BL_DM.Dim_categories dms
on ds.category  = dms.category
where dms.category is null;



merge into BL_DM.Dim_Categories dm_cat
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_CATEGORIES' as Source_entity,
sc.source_id  as Source_id,
sc.category  as Category,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.dim_categories sc) nf_cat on nf_cat.category = dm_cat.category
when not matched then
  insert ( Source_system, Source_entity, Source_id, Category, INSERT_DT, UPDATE_DT)
  values
  ( nf_cat.source_system,
    nf_cat.Source_entity,
    nf_cat.Source_id,
    nf_cat.Category,
    nf_cat.INSERT_DT,
    nf_cat.UPDATE_DT);

call bl_dm.dm_InsertLog( 'dm_cat', row_no::bigint , 'Procedure run succesfully', i_load_id);
commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Subcategories(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_subcategories ds
left join BL_DM.Dim_subcategories dms
on ds.subcategory  = dms.subcategory
where dms.subcategory is null;


merge into BL_DM.Dim_Subcategories dm_subcat
using (select
c1.pk_category_id as FK_Category_ID,
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_SUBCATEGORIES' as Source_entity,
sc.source_id  as Source_id,
sc.subcategory as Subcategory,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.Dim_Subcategories sc
left join bl_3nf.dim_categories c on c.pk_category_id = sc.fk_category_id
left join BL_DM.dim_categories c1 on c1.category = c.category
) nf_subcat on nf_subcat.Subcategory = dm_subcat.Subcategory
when not matched then
  insert (FK_Category_ID, Source_system, Source_entity, Source_id, Subcategory, INSERT_DT, UPDATE_DT)
  values
  ( nf_subcat.FK_Category_ID,
  	nf_subcat.source_system,
    nf_subcat.Source_entity,
    nf_subcat.Source_id,
    nf_subcat.Subcategory,
    nf_subcat.INSERT_DT,
    nf_subcat.UPDATE_DT);
 call bl_dm.dm_InsertLog( 'dm_subcat', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Dates(i_load_id int)
as $$
declare
    row_no bigint;
begin
WITH RECURSIVE DateSequence AS (
    SELECT
        1 AS PK_Date_SURR_ID,
        CAST('2022-01-01' AS DATE) AS DateValue
    UNION ALL
    SELECT
        PK_Date_SURR_ID + 1,
        CAST(DateValue + INTERVAL '1 day' AS DATE)
    FROM DateSequence
    WHERE DateValue < '2024-12-31'
)
INSERT INTO BL_DM.Dim_Dates (PK_Date_SURR_ID, Full_Date, Day, Month, Year, Quarter)
SELECT
    PK_Date_SURR_ID,
    CAST(DateValue AS DATE) AS Full_Date,
    EXTRACT(DAY FROM DateValue) AS Day,
    EXTRACT(MONTH FROM DateValue) AS Month,
    EXTRACT(YEAR FROM DateValue) AS Year,
    EXTRACT(QUARTER FROM DateValue) AS Quarter
FROM DateSequence
on conflict (Full_Date) do nothing;

commit;
end;
$$ language plpgsql;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Sectors(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_sectors ds
left join BL_DM.Dim_Sectors dms
on ds.sector = dms.sector
where dms.sector is null;

merge into BL_DM.Dim_Sectors dm_sector
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_SECTORS' as Source_entity,
sc.source_id  as Source_id,
sc.sector  as sector,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.dim_sectors  sc) nf_sector on nf_sector.sector = dm_sector.sector
when not matched then
  insert ( Source_system, Source_entity, Source_id, sector, INSERT_DT, UPDATE_DT)
  values
  ( nf_sector.source_system,
    nf_sector.Source_entity,
    nf_sector.Source_id,
    nf_sector.sector,
    nf_sector.INSERT_DT,
    nf_sector.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_sector', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Segments(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_segments ds
left join BL_DM.Dim_Segments dms
on ds.segment = dms.segment
where dms.segment is null ;

merge into BL_DM.Dim_Segments dm_segment
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_SEGMENTS' as Source_entity,
sc.source_id  as Source_id,
sc.Segment  as Segment,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.dim_segments sc) nf_segments on nf_segments.Segment = dm_segment.Segment
when not matched then
  insert ( Source_system, Source_entity, Source_id, Segment, INSERT_DT, UPDATE_DT)
  values
  ( nf_segments.source_system,
    nf_segments.Source_entity,
    nf_segments.Source_id,
    nf_segments.Segment,
    nf_segments.INSERT_DT,
    nf_segments.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_segment', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Markets(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_markets dm
left join BL_DM.Dim_Markets dmm
on dm.market = dmm.market
where dmm.market is null;

merge into BL_DM.Dim_Markets dm_markets
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_MARKETS' as Source_entity,
sc.source_id  as Source_id,
sc.market  as market,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.Dim_Markets sc) nf_market on nf_market.market = dm_markets.market
when not matched then
  insert ( Source_system, Source_entity, Source_id, market, INSERT_DT, UPDATE_DT)
  values
  ( nf_market.source_system,
    nf_market.Source_entity,
    nf_market.Source_id,
    nf_market.market,
    nf_market.INSERT_DT,
    nf_market.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_Markets', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create or replace procedure Dm_Orders(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_orders do2
left join BL_DM.Dim_Orders dmo
on do2.order_number = dmo.order_number
where dmo.order_number is null;

merge into BL_DM.Dim_Orders dm_orders
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_ORDERS' as Source_entity,
sc.source_id  as Source_id,
sc.Order_Number as Order_Number,
sc.Ship_Date as Ship_Date,
sc.Order_Priority as Order_Priority,
sc.ship_mode as Ship_Mode
from bl_3nf.Dim_Orders sc) nf_orders on nf_orders.Order_Number = dm_orders.Order_Number
when not matched then
  insert ( Source_system, Source_entity, Source_id, Order_Number, Ship_Date, Order_Priority, Ship_Mode)
  values
  ( nf_orders.source_system,
    nf_orders.Source_entity,
    nf_orders.Source_id,
    nf_orders.Order_Number,
    nf_orders.Ship_Date,
    nf_orders.Order_Priority,
    nf_orders.Ship_Mode);
   call bl_dm.dm_InsertLog( 'dm_orders', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Regions(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_regions dr
left join BL_DM.Dim_Regions dmdr
on dr.region = dmdr.region
where dmdr.region is null ;

merge into BL_DM.Dim_Regions dm_regions
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_REGIONS' as Source_entity,
sc.source_id  as Source_id,
sc.region  as region ,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.Dim_Regions sc) nf_region on nf_region.region = dm_regions.region
when not matched then
  insert ( Source_system, Source_entity, Source_id, region, INSERT_DT, UPDATE_DT)
  values
  ( nf_region.source_system,
    nf_region.Source_entity,
    nf_region.Source_id,
    nf_region.region,
    nf_region.INSERT_DT,
    nf_region.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_regions', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;



---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Countries(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_countries dc
left join BL_DM.Dim_Countries dmc
on dc.country = dmc.country
where dmc.country is null;

merge into BL_DM.Dim_Countries dm_countries
using (select
c1.pk_region_id  as FK_Region_ID,
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_COUNTRIES' as Source_entity,
sc.source_id  as Source_id,
sc.country  as Country,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.dim_countries sc
left join bl_3nf.dim_regions  c on c.pk_region_id  = sc.fk_region_id
left join BL_DM.dim_regions c1 on c1.region  = c.region
) nf_countries on nf_countries.country = dm_countries.country
when not matched then
  insert (fk_region_id, Source_system, Source_entity, Source_id, country, INSERT_DT, UPDATE_DT)
  values
  ( nf_countries.fk_region_id,
  	nf_countries.source_system,
    nf_countries.Source_entity,
    nf_countries.Source_id,
    nf_countries.country,
    nf_countries.INSERT_DT,
    nf_countries.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_countries', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Addresses(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_addresses da
left join BL_DM.Dim_Addresses dma
on da.shipping_address = dma.shipping_address
where dma.shipping_address is null;

merge into BL_DM.Dim_Addresses dm_addresses
using (select
c1.pk_country_id  as FK_Country,
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_ADDRESSES' as Source_entity,
sc.source_id  as Source_id,
Shipping_Address as Shipping_Address,
City as City,
Postal_Code as Postal_Code,
CAST(NOW() AS DATE) as INSERT_DT,
to_date('9999-12-31', 'YYYY-MM-DD') as UPDATE_DT
from bl_3nf.Dim_Addresses sc
left join bl_3nf.dim_countries  c on c.pk_country_id  = sc.fk_country
left join BL_DM.dim_countries c1 on c1.country  = c.country
) nf_addresses on nf_addresses.Shipping_Address = dm_addresses.Shipping_Address
when not matched then
  insert (fk_country, Source_system, Source_entity, Source_id, Shipping_Address,City,Postal_Code , INSERT_DT, UPDATE_DT)
  values
  ( nf_addresses.fk_country,
  	nf_addresses.source_system,
    nf_addresses.Source_entity,
    nf_addresses.Source_id,
    nf_addresses.Shipping_Address,
    nf_addresses.City,
    nf_addresses.Postal_Code,
    nf_addresses.INSERT_DT,
    nf_addresses.UPDATE_DT);
   call bl_dm.dm_InsertLog( 'dm_addresses', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Products(i_load_id int)
as $$
declare
    row_no bigint;
begin


select count(*) into row_no
from
bl_3nf.dim_products_scd2 dps
left join BL_DM.Dim_Products_SCD2 dmp
on dps.product_nr = dmp.product_nr
and dmp.is_active = true
where dmp.product_nr is null
and dps.is_active = true;

merge into BL_DM.Dim_Products_SCD2 dm_product
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_PRODUCT' as Source_entity,
sc.source_id  as Source_id,
sc.Product_NR as Product_NR,
sc.Product_name as Product_name,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Products_SCD2 sc
where sc.is_active = false
and sc.end_dt <> to_date('9999-12-31', 'YYYY-MM-DD')) nf_product on nf_product.Product_NR = dm_product.Product_NR
and  nf_product.Product_name = dm_product.Product_name
and  dm_product.END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
and  dm_product.IS_ACTIVE = true
when matched and nf_product.Product_name = dm_product.Product_name
and nf_product.Product_name = dm_product.Product_name
then
update
set end_dt = nf_product.END_DT,
    is_active = false;
 commit;


merge into BL_DM.Dim_Products_SCD2 dm_product
using (select
scat2.pk_subcategory_id  as FK_Subcategory_ID,
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_PRODUCT' as Source_entity,
sc.source_id  as Source_id,
sc.Product_NR as Product_NR,
sc.Product_name as Product_name,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Products_SCD2 sc
left join bl_3nf.dim_subcategories scat on scat.pk_subcategory_id = sc.FK_Subcategory_ID
left join BL_DM.dim_subcategories  scat2 on scat2.subcategory  = scat.subcategory
where sc.is_active = true
and sc.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')) nf_product on nf_product.Product_NR = dm_product.Product_NR
and  nf_product.Product_NR = dm_product.Product_NR
and  nf_product.Product_name = dm_product.Product_name
and  dm_product.END_DT=to_date('9999-12-31', 'YYYY-MM-DD')
when not matched then
  insert (FK_Subcategory_ID, Source_system, Source_entity, Source_id, Product_NR, Product_name, START_DT,END_DT,IS_ACTIVE)
  values
  ( nf_product.FK_Subcategory_ID,
  	nf_product.source_system,
    nf_product.Source_entity,
    nf_product.Source_id,
	nf_product.Product_NR,
	nf_product.Product_name,
	nf_product.START_DT,
	nf_product.END_DT,
	nf_product.IS_ACTIVE);
call bl_dm.dm_InsertLog( 'dm_product', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Customers(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_customers_scd2 dcs
left join BL_DM.Dim_Customers_SCD2 dmc
on dcs.customer_nr = dmc.customer_nr
and dmc.is_active = true
where dmc.customer_nr is null
and dcs.is_active = true ;

merge into BL_DM.Dim_Customers_SCD2 dm_customers
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_CUSTOMERS' as Source_entity,
sc.source_id  as Source_id,
sc.Customer_NR as Customer_NR,
sc.Tax_Number as Tax_Number,
sc.Contact_person as Contact_person,
sc.Customer_Name as Customer_Name,
sc.Email as Email,
sc.Gender as Gender,
sc.Age_Group as Age_Group,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Customers_SCD2 sc
--left join dim_segments sg on sg.pk_segment_id = dm_customers.fk_segment_id
--left join dim_sectors sct on sct.pk_sector_id = dm_customers.fk_sector_id
where sc.is_active = false
and sc.end_dt <> to_date('9999-12-31', 'YYYY-MM-DD')) nf_customer on nf_customer.Customer_NR = dm_customers.Customer_NR
and  nf_customer.Contact_person = dm_customers.Contact_person
and  nf_customer.Customer_Name = dm_customers.Customer_Name
and  nf_customer.Email = dm_customers.Email
and  nf_customer.Gender = dm_customers.Gender
and  nf_customer.Age_Group = dm_customers.Age_Group
and  dm_customers.END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
and  dm_customers.IS_ACTIVE = true
when matched and nf_customer.Customer_Name = dm_customers.Customer_Name
and nf_customer.Email = dm_customers.Email
and  nf_customer.Gender = dm_customers.Gender
and  nf_customer.Age_Group = dm_customers.Age_Group then
update
set end_dt = nf_customer.END_DT,
    is_active = false;
 commit;



merge into BL_DM.Dim_Customers_SCD2 dm_customers
using (select
sct2.pk_sector_id as FK_Sector_ID,
sg2.pk_segment_id as FK_Segment_ID,
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_CUSTOMERS' as Source_entity,
sc.source_id  as Source_id,
sc.Customer_NR as Customer_NR,
sc.Tax_Number as Tax_Number,
sc.Contact_person as Contact_person,
sc.Customer_Name as Customer_Name,
sc.Email as Email,
sc.Gender as Gender,
sc.Age_Group as Age_Group,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Customers_SCD2 sc
left join bl_3nf.dim_segments sg on sg.pk_segment_id = sc.fk_segment_id
left join BL_DM.dim_segments sg2 on sg2.pk_segment_id = sc.fk_segment_id
left join bl_3nf.dim_sectors sct on sct.pk_sector_id = sc.fk_sector_id
left join BL_DM.dim_sectors sct2 on sct2.pk_sector_id = sc.fk_sector_id
where sc.is_active = true
and sc.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')) nf_customer on nf_customer.Customer_NR = dm_customers.Customer_NR
and  nf_customer.Contact_person = dm_customers.Contact_person
and  nf_customer.Customer_Name = dm_customers.Customer_Name
and  nf_customer.Email = dm_customers.Email
and  nf_customer.Gender = dm_customers.Gender
and  nf_customer.Age_Group = dm_customers.Age_Group
and  dm_customers.END_DT=to_date('9999-12-31', 'YYYY-MM-DD')
when not matched then
  insert (FK_Sector_ID, FK_Segment_ID, Source_system, Source_entity, Source_id,  Customer_NR,Tax_Number, Contact_person, Customer_Name,Email,Gender,Age_Group,START_DT,END_DT,IS_ACTIVE)
  values
  ( nf_customer.FK_Sector_ID,
    nf_customer.FK_Segment_ID,
  	nf_customer.source_system,
    nf_customer.Source_entity,
    nf_customer.Source_id,
	nf_customer.Customer_NR,
	nf_customer.Tax_Number,
	nf_customer.Contact_person,
	nf_customer.Customer_Name,
	nf_customer.Email,
	nf_customer.Gender,
	nf_customer.Age_Group,
	nf_customer.START_DT,
	nf_customer.END_DT,
	nf_customer.IS_ACTIVE);
call bl_dm.dm_InsertLog( 'dm_customers', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_Employees(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.dim_employees_scd2 des
left join BL_DM.Dim_Employees_SCD2 dme
on des.employee_nr = dme.employee_nr
and dme.is_active = true
where dme.employee_nr is null
and des.is_active = true;

merge into BL_DM.Dim_Employees_SCD2 dm_employees
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_EMPLOYEES' as Source_entity,
sc.source_id  as Source_id,
sc.Employee_NR as Employee_NR,
sc.Employee_FirstName as Employee_FirstName,
sc.Employee_LastName as Employee_LastName,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Employees_SCD2 sc
where sc.is_active = false
and sc.end_dt <> to_date('9999-12-31', 'YYYY-MM-DD')) nf_employee on nf_employee.employee_nr = dm_employees.employee_nr
and  (nf_employee.Employee_FirstName = dm_employees.Employee_FirstName
or nf_employee.Employee_LastName = dm_employees.Employee_LastName)
and  dm_employees.END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
and  dm_employees.IS_ACTIVE = true
when matched and (nf_employee.Employee_FirstName = dm_employees.Employee_FirstName and nf_employee.Employee_LastName = dm_employees.Employee_LastName) then
update
set end_dt = nf_employee.END_DT,
    is_active = false;
 commit;


merge into BL_DM.Dim_Employees_SCD2 dm_employees
using (select
'GLOBAL_SUPERSTORE' as source_system,
'BL_3NF.DIM_EMPLOYEES' as Source_entity,
sc.source_id  as Source_id,
sc.Employee_NR as Employee_NR,
sc.Employee_FirstName as Employee_FirstName,
sc.Employee_LastName as Employee_LastName,
sc.START_DT as START_DT,
sc.END_DT as END_DT,
sc.IS_ACTIVE as IS_ACTIVE
from bl_3nf.Dim_Employees_SCD2 sc
where sc.is_active = true
and sc.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')) nf_employee on nf_employee.employee_nr = dm_employees.employee_nr
and  nf_employee.Employee_FirstName = dm_employees.Employee_FirstName
and  nf_employee.Employee_LastName = dm_employees.Employee_LastName
and  dm_employees.END_DT=to_date('9999-12-31', 'YYYY-MM-DD')
when not matched then
  insert ( Source_system, Source_entity, Source_id, Employee_NR,Employee_FirstName,Employee_LastName,START_DT,END_DT,IS_ACTIVE)
  values
  ( nf_employee.source_system,
    nf_employee.Source_entity,
    nf_employee.Source_id,
	nf_employee.Employee_NR,
	nf_employee.Employee_FirstName,
	nf_employee.Employee_LastName,
	nf_employee.START_DT,
	nf_employee.END_DT,
	nf_employee.IS_ACTIVE);
call bl_dm.dm_InsertLog( 'dm_employees', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure Dm_fact(i_load_id int)
as $$
declare
    row_no bigint;
begin

select count(*) into row_no
from
bl_3nf.fct_orders_dd fod
where fod.load_id = i_load_id;

merge into BL_DM.FCT_ORDERS_DD fct
using
(select
emp2.pk_employee_id as FK_Employee_ID,
dcs2.pk_customer_id as FK_Customer_ID,
da2.pk_address_id as FK_Shipping_Address_ID,
dps2.pk_product_id as FK_Product_ID,
dor2.pk_order_id as FK_Order_ID,
dm2.pk_market_id  as FK_Market_ID,
dates.pk_date_surr_id as FK_Date_ID,
sc.Sales as Sales,
sc.Quantity as Quantity,
sc.Discount as Discount,
sc.Profit as Profit,
sc.Shipping_Cost as Shipping_Cost
from bl_3nf.FCT_ORDERS_DD sc
left join (select * from bl_3nf.dim_employees_scd2 emp where emp.IS_ACTIVE = true) emp
on sc.fk_employee_id = emp.pk_employee_id
left join (select * from bl_dm.dim_employees_scd2 emp2 where emp2.IS_ACTIVE = true) emp2
 on emp.employee_nr  = emp2.employee_nr
left join (select * from bl_3nf.dim_customers_scd2 dcs where dcs.IS_ACTIVE = true) dcs
 on sc.FK_Customer_ID = dcs.pk_customer_id
left join (select * from bl_dm.dim_customers_scd2 dcs2 where dcs2.IS_ACTIVE = true) dcs2
 on dcs.customer_nr  = dcs2.customer_nr
left join bl_3nf.dim_addresses da on sc.FK_Shipping_Address_ID =da.pk_address_id
left join bl_dm.dim_addresses da2 on da.shipping_address  =da2.shipping_address
left join (select * from bl_3nf.dim_products_scd2 dps where dps.IS_ACTIVE = true) dps
  on sc.fk_product_id = dps.pk_product_id
left join (select * from bl_3nf.dim_products_scd2 dps2 where dps2.IS_ACTIVE = true) dps2
  on dps.product_nr  = dps2.product_nr
left join bl_3nf.dim_orders dor on sc.fk_order_id = dor.pk_order_id
left join bl_dm.dim_orders dor2 on dor.order_number  = dor2.order_number
left join bl_3nf.dim_markets dm on sc.fk_market_id = dm.pk_market_id
left join bl_dm.dim_markets dm2 on dm.market  = dm2.market
left join bl_dm.dim_dates dates on sc.order_date = dates.full_date
where sc.load_id = i_load_id
) dm_fct
on   dm_fct.FK_Employee_ID = fct.FK_Employee_ID
and   dm_fct.FK_Customer_ID = fct.FK_Customer_ID
and   dm_fct.FK_Shipping_Address_ID = fct.FK_Shipping_Address_ID
and  dm_fct.FK_Product_ID = fct.FK_Product_ID
and  dm_fct.FK_Order_ID = fct.FK_Order_ID
and  dm_fct.FK_Market_ID = fct.FK_Market_ID
and  dm_fct.FK_Date_ID = fct.FK_Date_ID
when not matched then
insert ( FK_Employee_ID ,FK_Customer_ID ,FK_Shipping_Address_ID ,FK_Product_ID ,FK_Order_ID ,FK_Market_ID ,FK_Date_ID ,Sales ,Quantity ,Discount,Profit ,Shipping_Cost )
  values
(
    dm_fct.FK_Employee_ID ,
    dm_fct.FK_Customer_ID ,
    dm_fct.FK_Shipping_Address_ID ,
    dm_fct.FK_Product_ID ,
    dm_fct.FK_Order_ID ,
    dm_fct.FK_Market_ID ,
    dm_fct.FK_Date_ID ,
    dm_fct.Sales ,
    dm_fct.Quantity ,
    dm_fct.Discount ,
    dm_fct.Profit ,
    dm_fct.Shipping_Cost
) ;

call bl_dm.dm_InsertLog( 'dm_fact', row_no::bigint , 'Procedure run succesfully', i_load_id);

commit;
end;
$$ language plpgsql;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



create or replace procedure bl_cl.run_dm()
as $$
declare
    row_no bigint;
declare
   v_load_id int;
begin
select load_id into v_load_id from bl_cl.current_load_id;
call bl_cl.dm_sectors(v_load_id);
call bl_cl.dm_segments(v_load_id);
call bl_cl.Dm_Customers(v_load_id);
call bl_cl.dm_regions(v_load_id);
call bl_cl.dm_countries(v_load_id);
call bl_cl.dm_addresses(v_load_id);
call bl_cl.dm_dates(v_load_id);
call bl_cl.dm_employees(v_load_id);
call bl_cl.dm_markets(v_load_id);
call bl_cl.dm_orders(v_load_id);
call bl_cl.dm_categories(v_load_id);
call bl_cl.dm_subcategories(v_load_id);
call bl_cl.dm_products(v_load_id);
call bl_cl.dm_fact(v_load_id);
end;
$$ language plpgsql;


----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


call bl_cl.run_dm();
select * from bl_dm.dm_log_table l;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select * from bl_cl.current_load_id;

select count(*) from bl_dm.Dim_Dates;
select count(*) from bl_dm.dim_sectors;
select count(*) from bl_dm.Dim_Segments;
select count(*) from bl_dm.Dim_Markets;
select count(*) from bl_dm.Dim_Orders;
select count(*) from bl_dm.Dim_Regions;
select count(*) from bl_dm.Dim_Countries;
select count(*) from bl_dm.Dim_Addresses;
select count(*) from bl_dm.Dim_Products_SCD2;
select count(*) from bl_dm.Dim_Customers_SCD2;
select count(*) from bl_dm.Dim_Employees_SCD2;
select count(*) from bl_dm.FCT_ORDERS_DD ;
select count(*) from bl_3nf.Dim_Categories;
select count(*) from bl_3nf.Dim_Sectors;
select count(*) from bl_3nf.Dim_Segments;
select count(*) from bl_3nf.Dim_Markets;
select count(*) from bl_3nf.Dim_Regions;
select count(*) from bl_3nf.Dim_Countries;
select count(*) from bl_3nf.Dim_Orders;
select count(*) from bl_3nf.Dim_Addresses;
select count(*) from bl_3nf.Dim_Subcategories;
select count(*) from bl_3nf.Dim_Products_SCD2;
select count(*) from bl_3nf.Dim_Customers_SCD2;
select count(*) from bl_3nf.Dim_Employees_SCD2;
select count(*) from bl_3nf.FCT_ORDERS_DD;

CREATE OR REPLACE FUNCTION bl_cl.check_for_duplicates(schema_name TEXT, table_name TEXT)
RETURNS TABLE (table_with_duplicates TEXT) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT %L::text
        FROM %I.%I
        GROUP BY source_id
        HAVING COUNT(*) > 1', table_name, schema_name, table_name);
END;
$$ LANGUAGE plpgsql;


SELECT check_for_duplicates('bl_3nf', 'dim_categories');
SELECT check_for_duplicates('bl_3nf', 'dim_sectors');
SELECT check_for_duplicates('bl_3nf', 'dim_segments');
SELECT check_for_duplicates('bl_3nf', 'dim_markets');
SELECT check_for_duplicates('bl_3nf', 'dim_regions');
SELECT check_for_duplicates('bl_3nf', 'dim_countries');
SELECT check_for_duplicates('bl_3nf', 'dim_orders');
SELECT check_for_duplicates('bl_3nf', 'dim_addresses');
SELECT check_for_duplicates('bl_3nf', 'dim_subcategories');
SELECT check_for_duplicates('bl_3nf', 'dim_products_scd2');
SELECT check_for_duplicates('bl_3nf', 'dim_customers_scd2');
SELECT check_for_duplicates('bl_3nf', 'dim_employees_scd2');

/*In the fact table cannot be duplicates, because of the primary key constraint - te primary key is composed by the foreign keys to the dimensions.*/

SELECT check_for_duplicates('bl_dm', 'dim_sectors');
SELECT check_for_duplicates('bl_dm', 'dim_segments');
SELECT check_for_duplicates('bl_dm', 'dim_markets');
SELECT check_for_duplicates('bl_dm', 'dim_orders');
SELECT check_for_duplicates('bl_dm', 'dim_regions');
SELECT check_for_duplicates('bl_dm', 'dim_countries');
SELECT check_for_duplicates('bl_dm', 'dim_addresses');
SELECT check_for_duplicates('bl_dm', 'dim_products_scd2');
SELECT check_for_duplicates('bl_dm', 'dim_customers_scd2');
SELECT check_for_duplicates('bl_dm', 'dim_employees_scd2');
SELECT check_for_duplicates('bl_dm', 'dim_subcategories');
SELECT check_for_duplicates('bl_dm', 'dim_categories');

REFRESH MATERIALIZED VIEW bl_dm.yearly_sales_profit;

select * from  bl_dm.yearly_sales_profit ysp ;

REFRESH MATERIALIZED VIEW  bl_dm.sales_employees_regions;

select * from  bl_dm.sales_employees_regions ;

select * from bl_dm.dim_categories dc ;
