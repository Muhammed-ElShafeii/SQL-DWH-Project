-- DataWarehouse Project Query 1
use master; 
if exists (select 1 from sys.databases where name = 'DataWarehouse') -- SYS.DATABASES --> system catalog view that contains all databases in SQL Server.
begin
	alter database DataWarehouse set Single_User with rollback immediate; -- Single_User --> It forces the database to allow only one user connection. Why ? Because you cannot drop a database if someone is connected to it.
	-- Rollback imediate --> It: Immediately disconnects all users , Rolls back any running transactions, Frees the database instantly
	drop database DataWarehouse;
end;
go
use master;
go
alter database DataWarehouse 
set multi_user;
go
create database DataWarehouse;
use DataWarehouse;
go
create schema bronze; --creating layers (schemas: Gold, Silver and bronze)
go
create schema silver;
go
create schema gold;
go

use DataWarehouse

-- Datawarehouse Bronze layer Code

IF OBJECT_ID ('bronze.crm_cust_info', 'U') is not null 
	DROP Table bronze.crm_cust_info; --if the table exists in DB Go Drop the table from the DB and then we will create the table from scratch , go check each table before creation but i do it once. 
create table bronze.crm_cust_info --> Layer.<sourcesystem>_<entity>
(
cst_id int , 
cst_key nvarchar(50),
cst_firstname nvarchar(50), 
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date Date
);

create table bronze.crm_sales_details
(
sls_ord_num int,
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)

create table bronze.crm_prd_info 
(
prd_id int, 
prd_key nvarchar(50),
prd_nm nvarchar (50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt datetime,
prd_end_dt datetime
);

create table bronze.erp_CUST_AZ12
(
CID nvarchar(50),
BDATE datetime,
Gen nvarchar(50)
)

create table bronze.erp_LOC_A101
(
CID nvarchar(50),
CNTRY nvarchar(50)
)

create table bronze.erp_PX_CAT_G1V2
(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE NVARCHAR (50)
)

create or alter procedure bronze.load_bronze
as begin 
	Declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime
	begin try
		set @batch_start_time = getdate ()
		print '***************************************************';
		print 'Loading bronze layer';
		print '***************************************************';

		print '---------------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------------';



		set @start_time = getdate()
		Truncate table bronze.crm_cust_info; --making table empty
		BULK INSERT bronze.crm_cust_info --inserting 
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock -- will lock the whole table while loading
		);
		set @end_time = GETDATE()
		print '>> Load duration: ' + cast(datediff(second,@start_time, @end_time) as nvarchar) + 'seconds';

		select * from bronze.crm_cust_info
		select count(*) as Columns_Count from bronze.crm_cust_info
		---------------------------------------------------------------------------


		set @start_time = getdate()
		Truncate table bronze.crm_prd_info;
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		Firstrow = 2,
		fieldterminator = ',',
		tablock);
		set @end_time = getdate()
		print '>> Load duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		select count(*) as columns_count from bronze.crm_prd_info 
		---------------------------------------------------------------------------------

		Truncate table bronze.crm_sales_details;
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		Firstrow = 2,
		fieldterminator = ',',
		tablock);
		select count(*) as  columns_count from bronze.crm_sales_details

		-------------------------------------------------------------------------------------

		print '---------------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------------';

		truncate table bronze.erp_CUST_AZ12;
		bulk insert bronze.erp_CUST_AZ12
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		Firstrow = 2,
		fieldterminator = ',',
		tablock);
		select count(*) as columns_count from bronze.erp_CUST_AZ12

		-----------------------------------------------------------------------------------------

		truncate table bronze.erp_LOC_A101;
		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		Firstrow = 2,
		fieldterminator = ',',
		tablock);
		select count(*) as columns_count from bronze.erp_LOC_A101

		----------------------------------------------------------------------------------------------
		truncate table bronze.erp_PX_CAT_G1V2;
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\Cyber\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		Firstrow = 2,
		fieldterminator = ',',
		tablock);
		select count(*) as columns_count from bronze.erp_PX_CAT_G1V2
		-------------------------------------------------------------------------------------------------
	set @batch_end_time = getdate ()
	print '=================================================================================';
	print 'Loading Bronze is completed';
	print 'Total Loading time is: ' + cast(datediff (second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds'; 
	print '=================================================================================';
	
	end try
	begin catch
		print '============================================================================='
		print 'Error occured during loading bronze layer'
		print 'Error Message' + Error_Message();
		print 'Error Number' + cast(Error_Number() as nvarchar);
		print 'Error Number' + cast(Error_State() as nvarchar);
		print '============================================================================='
	end catch
end;

