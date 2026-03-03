select * from bronze.crm_prd_info
-- Table Prd_info
-- Check for nulls and duplicates of PK 
select prd_id , count(*) as Group_Count from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

-- check for unwanting spaces 
select prd_key , prd_nm , prd_line from bronze.crm_prd_info
where prd_key != trim(prd_key) or prd_nm != trim(prd_nm) or prd_line != trim(prd_line)

-- check for data consistency 
select distinct prd_line from bronze.crm_prd_info

-- Extracting information from column prd_key
select prd_id , prd_key , replace(substring(prd_key, 1, 5), '-', '_') as cat_id, substring(prd_key, 7, len(prd_key)) as prd_key , prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt from bronze.crm_prd_info

--check for cost values 
select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Handling null cost values and prd_line abbreviated values
select prd_id , prd_key , replace(substring(prd_key, 1, 5), '-', '_') as cat_id, substring(prd_key, 7, len(prd_key)) as prd_key , prd_nm, isnull(prd_cost, 0) as prd_cost ,  
case upper(trim(prd_line))
	 when  'M' then 'Mountain' 
	 when  'R' then 'Road'
	 when  'S' then 'Other Sales' 
	 when  'T' then 'Touring' 
	 else 'n/a'
end as prd_line,
prd_start_dt, prd_end_dt 
from bronze.crm_prd_info

-- Fixing date problem becuase the start date is bigger than the end date 
select prd_id , replace(substring(prd_key, 1, 5), '-', '_') as cat_id, substring(prd_key, 7, len(prd_key)) as prd_key , prd_nm, isnull(prd_cost, 0) as prd_cost ,  
case upper(trim(prd_line))
	 when  'M' then 'Mountain' 
	 when  'R' then 'Road'
	 when  'S' then 'Other Sales' 
	 when  'T' then 'Touring' 
	 else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt 
from bronze.crm_prd_info
  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------

  -- Table Cust_info 
-- Check for nulls and duplicates in primary key 
select count(*) as Duplicates_PK , cst_id from bronze.crm_cust_info
group by cst_id 
having count(cst_id) > 1 or cst_id is null

-- Handling duplicates PK 
select * from (
select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info) as t 
where flag_last = 1 

--check for unwanting spaces 
select cst_firstname from bronze.crm_cust_info
where cst_firstname != trim (cst_firstname) 

--then solving that 
select cst_id, cst_key,trim(cst_firstname) as cst_firstname , trim (cst_lastname) as cst_lastname , cst_marital_status, cst_gndr, cst_create_date from (
select * , row_number () over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id is not null) as t where flag_last = 1

-- Check data consistency 
select distinct cst_gndr from bronze.crm_cust_info

-- handling abbreviation terms 
select cst_id, cst_key, trim (cst_firstname) as cst_firstname , trim (cst_lastname) as cst_Lastname , cst_marital_status,  
case when upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when upper (trim(cst_marital_status)) = 'M' then 'Married'
	 else 'n/a'
end cst_marital_status,
case 
	when upper(trim(cst_gndr)) = 'M' then 'Male'
	when upper(trim(cst_gndr)) = 'F' then 'Female'
	else 'n/a'
end cst_gndr , 
cst_create_date from 
( select * , row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id is not null) as t where flag_last = 1

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Cleaning crm_sales_details table
select sls_ord_num, sls_prd_key, sls_cust_id, 
case when sls_order_dt = 0 or len(sls_order_dt) !=8 then NULL
	 else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then NULL
	 else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt, 
case when sls_due_dt = 0 or len(sls_due_dt) !=8 then NULL
	 else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt, 
sls_quantity ,
case when sls_price is null or sls_price <= 0
	 then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales
from bronze.crm_sales_details


-- Check for invalid dates 
select nullif(sls_order_dt,0) as sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0 or len(sls_order_dt) != 8

use Datawarehouse
select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check for sls_sales , sls_quantity , sls_price 
-- Sales = Quantity * Price 
-- values must not be null , zero, negative 

select
sls_quantity ,
case when sls_price is null or sls_price <= 0
	 then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null or sls_sales <= 0
or sls_quantity <= 0 or sls_sales <=0 
order by sls_sales 

-- Final select 
select sls_ord_num, sls_prd_key, sls_cust_id, 
case when sls_order_dt = 0 or len(sls_order_dt) !=8 then NULL
	 else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then NULL
	 else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt, 
case when sls_due_dt = 0 or len(sls_due_dt) !=8 then NULL
	 else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt, 
sls_quantity ,
case when sls_price is null or sls_price <= 0
	 then sls_sales / nullif(sls_quantity,0)
	 else sls_price
end as sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price) then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales
from bronze.crm_sales_details

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

--- Checking data in erp_cust_az12
select 
case when cid like 'NAS%' then substring (cid, 4, len(cid))
	 else cid 
end as cid, 
case when bdate > getdate () then null 
	 else bdate
end as bdate, 
case when trim(upper(gen)) in ('M', 'MALE') then 'Male'
	 when trim(upper(gen)) in ('F', 'FEMALE') then 'Female'
	 else'n/a'
end as gen
from bronze.erp_CUST_AZ12; 

-- check for invalid dates older than 100 years or born after getdate () 
select bdate from bronze.erp_CUST_AZ12
where bdate < '1924-01-01' or bdate > getdate ()

-- check for gen 
select distinct gen from bronze.erp_CUST_AZ12

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

select cid, cntry from bronze.erp_LOC_A101
select cst_key from silver.crm_cust_info

-- select  
select replace(cid,'-','') as cid , 
case when trim(upper(cntry)) in ('UNITED STATES', 'US', 'USA') then 'United States'
	 when trim (upper(cntry)) in ('GERMANY', 'DE') then 'Germany'
	 when trim(cntry) is null or trim(cntry) = '' then 'n/a'
	 else trim(cntry)
end as cntry
from bronze.erp_LOC_A101;
-- Data consistency 
select distinct cntry from bronze.erp_LOC_A101
order by cntry 
-- PK Different format 
select replace (cid,'-','') as cid from bronze.erp_LOC_A101

-------------------------------------------------------------------------------------------------------------------------------------------------------------------
