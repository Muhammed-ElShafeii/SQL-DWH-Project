select
	ci.cst_id , 
	ci.cst_key, 
	ci.cst_firstname, 
	ci.cst_lastname, 
	ci.cst_marital_status, 
	ci.cst_gndr, 
	ci.cst_create_date ,
	ca.BDATE,
	ca.Gen,
	la.CNTRY
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
on ci.cst_key = la.CID


-- Data Integration Customers Table 
select
	row_number () over (order by cst_id) as Customer_key,
	ci.cst_id as Customer_id, 
	ci.cst_key as Customer_Number, 
	ci.cst_firstname as First_Name, 
	ci.cst_lastname as Last_Name, 
	la.CNTRY as Country,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr 
		 else coalesce(ca.gen, 'n/a')
	end as Gender, 
	ci.cst_marital_status as Marital_Status, 
	cast(ca.BDATE as Date) as Birth_Date,
	ci.cst_create_date as Create_Date
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
on ci.cst_key = la.CID

-- Creating the object (view) Customers View Virtual Table.
create view gold.dim_customers as 
select
	row_number () over (order by cst_id) as Customer_key,
	ci.cst_id as Customer_id, 
	ci.cst_key as Customer_Number, 
	ci.cst_firstname as First_Name, 
	ci.cst_lastname as Last_Name, 
	la.CNTRY as Country,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr 
		 else coalesce(ca.gen, 'n/a')
	end as Gender, 
	ci.cst_marital_status as Marital_Status, 
	cast(ca.BDATE as Date) as Birth_Date,
	ci.cst_create_date as Create_Date
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 ca
on ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
on ci.cst_key = la.CID;


-- Products Table 
create or alter view gold.dim_products as 
select 
	ROW_NUMBER() over (order by pn.prd_start_dt , pn.prd_key) as Product_Key,
	pn.prd_id as Product_Id,
	pn.prd_key as Product_Number,
	pn.prd_nm as Product_Name,
	pn.cat_id as Category_Id,
	pc.CAT as Category,
	pc.SUBCAT as Sub_Category,
	pc.MAINTENANCE as Maintenance,
	pn.prd_cost as Cost,
	pn.prd_line as Product_Line,
	pn.prd_start_dt as Start_Date
from silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
on pn.cat_id = pc.ID
where pn.prd_end_dt is null -- Using the current products only (filtering out historical data) 

-- Sales Table
create or alter view gold.fact_sales as 
select 
	sd.sls_ord_num as Order_Number,
	pr.Product_Key,
	cu.Customer_key,
	sd.sls_order_dt as Order_Date,
	sd.sls_ship_dt as Shipping_Date,
	sd.sls_due_dt as Due_Date,
	sd.sls_price as Price,
	sd.sls_quantity as Quantity,
	sd.sls_sales as Sales_Amount
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.Product_Number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.Customer_id

-- Checking all data model 
select * from gold.fact_sales f 
left join gold.dim_customers c 
on c.Customer_key = f.Customer_key
left join gold.dim_products p
on p.Product_Key = f.Product_Key
where c.Customer_key is null or p.Product_Key is null



