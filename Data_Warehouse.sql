--********************************///////////////////*****__|||__*****//////////////////////********************************--

--*******************************************SQL Data Warehouse*******************************************--

--********************************///////////////////*****__|||__*****//////////////////////********************************--

-- Create Database 'DataWarehouse'

Use master;

Create Database DataWarehouse;

Use Datawarehouse;

Create Schema bronze;
go
Create Schema silver;
go
Create Schema gold;
go

///////////////////////////////////////*********___Bronze Layer___*********///////////////////////////////////////

If OBJECT_ID ('bronze.crm_cust_info','U') is not null
	Drop Table bronze.crm_cust_info;
Create Table bronze.crm_cust_info ( 
cst_id Int,
cst_key Nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date Date
);

If OBJECT_ID ('bronze.crm_prd_info','U') is not null
	Drop Table bronze.crm_prd_info;
Create Table bronze.crm_prd_info (
prd_id Int,
prd_key Nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt Datetime,
prd_end_dt Datetime
);

If OBJECT_ID ('bronze.crm_sales_details','U') is not null
	Drop Table bronze.crm_sales_details;
Create Table bronze.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);

If OBJECT_ID ('bronze.erp_loc_a101', 'U') is not null
	Drop Table bronze.erp_loc_a101;
Create Table bronze.erp_loc_a101 (
cid nvarchar(50),
cntry nvarchar(50)
);

If OBJECT_ID ('bronze.erp_cust_az12', 'U') is not null
	Drop Table bronze.erp_cust_az12;
Create Table bronze.erp_cust_az12 (
cid nvarchar(50),
bdate Date,
gen Nvarchar(50)
);

If OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') is not null
	Drop Table bronze.erp_px_cat_g1v2;
Create Table bronze.erp_px_cat_g1v2 (
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
);

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';

	Truncate Table bronze.crm_cust_info;
	PRINT '>> Inserting Data Into: bronze.crm_cust_info';
	Bulk Insert bronze.crm_cust_info
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';

	Truncate Table bronze.crm_prd_info;
	PRINT '>> Inserting Data Into: bronze.crm_prd_info';
	Bulk Insert bronze.crm_prd_info
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';

	Truncate Table bronze.crm_sales_details;
	PRINT '>> Inserting Data Into: bronze.crm_sales_details';
	Bulk Insert bronze.crm_sales_details
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
	);

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';

	Truncate Table bronze.erp_loc_a101;
	PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
	Bulk Insert bronze.erp_loc_a101
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';

	Truncate Table bronze.erp_cust_az12;
	PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
	Bulk Insert bronze.erp_cust_az12
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

	Truncate Table bronze.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	Bulk Insert bronze.erp_px_cat_g1v2
	From 'C:\Users\siyae\OneDrive\Documents\My Data Sources\Data Warehouse Data\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (
		Firstrow = 2,
		Fieldterminator = ',',
		Tablock 
	);
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

///////////////////////////////////////*********Executing the Bronze Layer*********///////////////////////////////////////
EXEC bronze.load_bronze

Select Top 1000 * From bronze.crm_cust_info
Select Top 1000 * From bronze.crm_prd_info
Select Top 1000 * From bronze.crm_sales_details

Select Top 1000 * From bronze.erp_cust_az12
Select Top 1000 * From bronze.erp_loc_a101
Select Top 1000 * From bronze.erp_px_cat_g1v2

///////////////////////////////////////*********___Silver Layer___*********///////////////////////////////////////

If OBJECT_ID ('silver.crm_cust_info','U') is not null
	Drop Table silver.crm_cust_info;
Create Table silver.crm_cust_info ( 
cst_id Int,
cst_key Nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date Date,
dwh_create_date Datetime2 Default Getdate()
);

If OBJECT_ID ('silver.crm_prd_info','U') is not null
	Drop Table silver.crm_prd_info;
Create Table silver.crm_prd_info (
prd_id Int,
prd_key Nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt Datetime,
prd_end_dt Datetime,
dwh_create_date Datetime2 Default Getdate()
);

If OBJECT_ID ('silver.crm_sales_details','U') is not null
	Drop Table silver.crm_sales_details;
Create Table silver.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date Datetime2 Default Getdate()
);

If OBJECT_ID ('silver.erp_loc_a101', 'U') is not null
	Drop Table silver.erp_loc_a101;
Create Table silver.erp_loc_a101 (
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date Datetime2 Default Getdate()
);

If OBJECT_ID ('silver.erp_cust_az12', 'U') is not null
	Drop Table silver.erp_cust_az12;
Create Table silver.erp_cust_az12 (
cid nvarchar(50),
bdate Date,
gen Nvarchar(50),
dwh_create_date Datetime2 Default Getdate()
);

If OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') is not null
	Drop Table silver.erp_px_cat_g1v2;
Create Table silver.erp_px_cat_g1v2 (
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date Datetime2 Default Getdate()
);

--Updating Silver Layer (cust_info)------------------------------------------------------------------------------------------
-- Check for Nulls or Duplicates in Primary Key
Select cst_id,count(*)
From bronze.crm_cust_info
Group by cst_id
Having Count(*) > 1

Select * from 
( Select *, ROW_NUMBER() Over (Partition by cst_id Order By cst_create_date Desc) as flag_last
from bronze.crm_cust_info)t where flag_last = 1

-- Check for Unwanted Spaces
Select cst_firstname from
bronze.crm_cust_info where
cst_firstname != Trim(cst_firstname)

Select cst_lastname from
bronze.crm_cust_info where
cst_lastname != Trim(cst_lastname)

Select cst_id,cst_key, 
Trim(cst_firstname) as cst_firstname,
Trim(cst_lastname) as cst_lastname,

	Case When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
		When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
		Else 'n/a'
	End cst_marital_status,

	Case When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
		When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
		Else 'n/a'
	End cst_gndr,
	cst_create_date
	From (
		Select *, Row_Number() Over (Partition By cst_id Order By cst_create_date Desc) as flag_last
		from bronze.crm_cust_info where cst_id is Not Null)t where flag_last = 1 

-- Inserting the Data into Silver Layer(cust_info)
Insert into silver.crm_cust_info (
	cst_id,cst_key,cst_firstname,
	cst_lastname,cst_marital_status,
	cst_gndr,cst_create_date)

Select cst_id,cst_key, 
Trim(cst_firstname) as cst_firstname,
Trim(cst_lastname) as cst_lastname,

	Case When Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
		When Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
		Else 'n/a'
	End cst_marital_status,

	Case When Upper(Trim(cst_gndr)) = 'F' Then 'Female'
		When Upper(Trim(cst_gndr)) = 'M' Then 'Male'
		Else 'n/a'
	End cst_gndr,
	cst_create_date
	From (
		Select *, Row_Number() Over (Partition By cst_id Order By cst_create_date Desc) as flag_last
		from bronze.crm_cust_info where cst_id is Not Null)t where flag_last = 1 

Select Top 1000 * From silver.crm_cust_info

--Updating Silver Layer (prd_info)------------------------------------------------------------------------------------------------
-- Check for Nulls or Duplicates in Primary Key
Select prd_id, Count(*)
from bronze.crm_prd_info
Group By prd_id Having Count(*) > 1 or prd_id is Null

-- Extracting Category id from Primary Key
Select Top 1000 * From bronze.erp_px_cat_g1v2	
Select Top 1000 * From bronze.crm_prd_info

--(To Map id from erp & crm need to extract cat_id)

Select prd_id, prd_key,
Replace(Substring(prd_key,1,5), '-','_') as cat_id,
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
From bronze.crm_prd_info

--Checking out unmatched data after applying transformation
Select prd_id,prd_key,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
From bronze.crm_prd_info
Where Replace(Substring(prd_key,1,5),'-','_') Not In
(Select distinct id from bronze.erp_px_cat_g1v2)

--(To Map id from erp & crm from Sales)

Select Top 1000 * From bronze.crm_sales_details

Select prd_id,prd_key,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,                           --Len(prd_key) to know rest of characters
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
From bronze.crm_prd_info

--Checking out unmatched data after applying transformation
Select prd_id,prd_key,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,
prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
From bronze.crm_prd_info
Where Substring(prd_key,7,Len(prd_key)) Not In
(Select sls_prd_key From bronze.crm_sales_details)

--Checking for Nulls or Negative Numbers
Select prd_cost 
From bronze.crm_prd_info
Where prd_cost < 0 Or prd_cost Is Null

--Replacing Null as 0 & Changing prd_line details
Select prd_id,prd_key,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,
prd_nm,ISNULL(prd_cost,0) as prd_cost,
Case When Upper(Trim(prd_line)) = 'M' Then 'Mountain'
	 When Upper(Trim(prd_line)) = 'R' Then 'Road'
	 When Upper(Trim(prd_line)) = 'S' Then 'Other Sales'
	 When Upper(Trim(prd_line)) = 'T' Then 'Touring'
	 Else 'n/a'
End As prd_line,
prd_start_dt,prd_end_dt
From bronze.crm_prd_info
Where Substring(prd_key,7,Len(prd_key)) Not In
(Select sls_prd_key From bronze.crm_sales_details)

--Quick Case When Ideal for Simple Value mapping
Select prd_id,prd_key,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,
prd_nm,ISNULL(prd_cost,0) as prd_cost,
Case Upper(Trim(prd_line))
	When 'M' Then 'Mountain'
	When 'R' Then 'Road'
	When 'S' Then 'Other Sales'
	When 'T' Then 'Touring'
	Else 'n/a'
End As prd_line,
prd_start_dt,prd_end_dt
From bronze.crm_prd_info
Where Substring(prd_key,7,Len(prd_key)) Not In
(Select sls_prd_key From bronze.crm_sales_details)

--Checking for Invalid Date Orders
Select * From bronze.crm_prd_info
Where prd_end_dt < prd_start_dt               --End date Should be End Date = Start Date of Next Record -1

--Replacing End Date
Select prd_id,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,
prd_nm,
ISNULL (prd_cost,0) as prd_cost,
Case Upper(Trim(prd_line))
	When 'M' Then 'Mountain'
	When 'R' Then 'Road'
	When 'S' Then 'Other Sales'
	When 'T' Then 'Touring'
	Else 'n/a'
End as prd_line,prd_start_dt,
Lead(prd_start_dt) Over (Partition By prd_key Order By prd_start_dt) -1 as prd_end_dt
From bronze.crm_prd_info

--Updating the Silver layer(prd_info)
If OBJECT_ID ('silver.crm_prd_info','U') Is Not Null
	Drop Table silver.crm_prd_info;
Create Table silver.crm_prd_info (
	prd_id Int,
	cat_id Nvarchar(50),
	prd_key Nvarchar(50),
	prd_nm Nvarchar(50),
	prd_cost Int,
	prd_line Nvarchar(50),
	prd_start_dt Date,
	prd_end_dt Date,
	dwh_create_date Datetime2 default Getdate()
);

--Inserting into Silver Layer(prd_info)
Insert Into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt ) 
Select prd_id,
Replace(Substring(prd_key,1,5),'-','_') as cat_id,
Substring(prd_key,7,Len(prd_key)) as prd_key,
prd_nm,
ISNULL (prd_cost,0) as prd_cost,
Case Upper(Trim(prd_line))
	When 'M' Then 'Mountain'
	When 'R' Then 'Road'
	When 'S' Then 'Other Sales'
	When 'T' Then 'Touring'
	Else 'n/a'
End as prd_line,prd_start_dt,
Lead(prd_start_dt) Over (Partition By prd_key Order By prd_start_dt) -1 as prd_end_dt
From bronze.crm_prd_info

Select Top 100 * From silver.crm_prd_info

--Updating the Silver layer(sales_details)--------------------------------------------------------------------------------------
Select * from bronze.crm_sales_details

--Checking for Invalid Dates
Select sls_order_dt
From bronze.crm_sales_details
Where sls_order_dt <= 0

--Replacing 0 with Null
Select Nullif(sls_order_dt,0) sls_order_dt
From bronze.crm_sales_details
Where sls_order_dt <= 0

--Checking Sales price
Select Distinct sls_sales,sls_quantity,sls_price
From bronze.crm_sales_details
Where sls_sales != sls_quantity * sls_price
Or sls_sales Is Null Or sls_quantity Is Null Or sls_price Is Null
Or sls_sales <= 0 Or sls_quantity <= 0 Or sls_price <=0
Order By sls_sales,sls_quantity,sls_price

--Updating the Silver layer(sales_details)
If OBJECT_ID ('silver.crm_sales_details','U') Is Not Null
	Drop Table silver.crm_sales_details;
Create Table silver.crm_sales_details (
	sls_ord_num Nvarchar(50),
	sls_prd_key Nvarchar(50),
	sls_cust_id Int,
	sls_order_dt Date,
	sls_ship_dt Date,
	sls_due_dt Date,
	sls_sales Int,
	sls_quantity Int,
	sls_price Int,
	dwh_create_date Datetime2 default Getdate()
);

--Inserting into Silver Layer(sales_details)
Insert Into silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price )
Select sls_ord_num, sls_prd_key, sls_cust_id,
Case When sls_order_dt = 0 Or Len(sls_order_dt) != 8 Then Null
	Else Cast(Cast(sls_order_dt as Varchar) As Date)               --Sls_dt was in Int, directly cant convert to Date so.
End As sls_order_dt,
Case When sls_ship_dt = 0 Or Len(sls_ship_dt) != 8 Then Null
	Else Cast(Cast(sls_ship_dt as Varchar) As Date)               
End As sls_ship_dt,
Case When sls_due_dt = 0 Or Len(sls_due_dt) != 8 Then Null
	Else Cast(Cast(sls_due_dt as Varchar) As Date)               
End As sls_due_dt,
Case When sls_sales is Null Or sls_sales <=0 Or sls_sales != sls_quantity * Abs(sls_price)
	Then sls_quantity * Abs(sls_price)
Else sls_sales
End As sls_sales,
sls_quantity,
Case When sls_price is Null Or sls_price <= 0
		Then sls_sales / Nullif(sls_quantity,0)
	Else sls_price
End As sls_price
From bronze.crm_sales_details

Select * From silver.crm_sales_details

--Updating the Silver layer(erp_cust_az12)--------------------------------------------------------------------------------------------
Select * From bronze.erp_cust_az12

--Checking the cid
Select cid,
Case When cid Like 'NAS%' Then Substring(cid, 4, Len(cid))
	Else cid
End cid,
bdate,gen
From bronze.erp_cust_az12

--Checking for valid Date
Select Distinct bdate
From bronze.erp_cust_az12
Where bdate < '1924-01-01' Or bdate > Getdate()      --Getdate() is Present date

--Updating the Silver layer(erp_cust_az12)
Select 
Case When cid Like 'NAS%' Then Substring(cid,4,Len(cid))
	Else cid
End As cid,
Case When bdate > Getdate() Then Null
	Else bdate
End As bdate,
Case When Upper(Trim(gen)) IN ('F','Female') Then 'Female'
	When Upper(Trim(gen)) IN ('M','Male') Then 'Male'
	Else 'n/a'
End As gen
From bronze.erp_cust_az12

--Inserting into Silver Layer(erp_cust_az12)
Insert Into 
silver.erp_cust_az12 (cid,bdate,gen)
Select 
Case When cid Like 'NAS%' Then Substring(cid,4,Len(cid))
	Else cid
End As cid,
Case When bdate > Getdate() Then Null
	Else bdate
End As bdate,
Case When Upper(Trim(gen)) IN ('F','Female') Then 'Female'
	When Upper(Trim(gen)) IN ('M','Male') Then 'Male'
	Else 'n/a'
End As gen
From bronze.erp_cust_az12

Select * from silver.erp_cust_az12

--Updating the Silver Layer(erp_loc_a101)-----------------------------------------------------------------------------------------------------------------
Select * From bronze.erp_loc_a101

--Checking cid
Select Replace(cid,'-','') cid,cntry
From bronze.erp_loc_a101

--Updating the Silver Layer(erp_loc_a101)
Select Replace(cid,'-','') cid,
Case When Trim(cntry) = 'DE' Then 'Germany'
	When Trim(cntry) IN ('US', 'USA') Then 'United States'
	When Trim(cntry) = '' Or cntry Is Null Then 'n/a'
	else Trim(cntry)
End As cntry
From bronze.erp_loc_a101

--Inserting Into Silver Layer(erp_loc_a101)
Insert Into silver.erp_loc_a101
(cid,cntry)
Select Replace(cid,'-','') cid,
Case When Trim(cntry) = 'DE' Then 'Germany'
	When Trim(cntry) IN ('US', 'USA') Then 'United States'
	When Trim(cntry) = '' Or cntry Is Null Then 'n/a'
	else Trim(cntry)
End As cntry
From bronze.erp_loc_a101

Select * from silver.erp_loc_a101

--Updating the Silver Layer(px_cat_g1v2)---------------------------------------------------------------------------------------
Select * from bronze.erp_px_cat_g1v2

--Check for Unwanted Spaces
Select * from bronze.erp_px_cat_g1v2
Where cat != Trim(cat) or subcat != Trim(subcat)

--Data Standardization & Consistency
Select Distinct cat
From bronze.erp_px_cat_g1v2

--No Changes required for this table

--Inserting Into Silver Layer(px_cat_g1v2)
Insert into silver.erp_px_cat_g1v2
(id,cat,subcat,maintenance)
Select id,cat,subcat,maintenance
from bronze.erp_px_cat_g1v2

-----------------///------------***--------------Inserting all the Data into Silver Table-----------------///------------***--------------
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END

///////////////////////////////////////*********Executing the Silver Layer*********///////////////////////////////////////
EXEC silver.load_silver

///////////////////////////////////////*********___Gold Layer___*********///////////////////////////////////////

--Updating the Gold Layer (cust_info)-----------------------------------------------------------------------------------------------------------
Select * From silver.crm_cust_info

Select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 as la
On ci.cst_key = la.cid

--Checking whether Duplicate data is present
Select cst_id, Count(*) From 
	(Select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 as la
On ci.cst_key = la.cid
)t Group By cst_id
Having Count(*)>1

--Data Integration (Assuming that Crm data is authentic than ERP)
Select Distinct
	ci.cst_gndr,
	ca.gen,
	Case when ci.cst_gndr != 'n/a' Then ci.cst_gndr
		Else COALESCE(ca.gen, 'n/a')
	End as new_gen
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
Order By 1,2

--Updating Gold layer(cust_info)
Select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	Case when ci.cst_gndr != 'n/a' Then ci.cst_gndr
		Else COALESCE(ca.gen, 'n/a')
	End as new_gen,
	ci.cst_create_date,
	ca.bdate,
	la.cntry
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 as la
On ci.cst_key = la.cid

--Renaming the Columns
Select 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	Case when ci.cst_gndr != 'n/a' Then ci.cst_gndr
		Else COALESCE(ca.gen, 'n/a')
	End as gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 as la
On ci.cst_key = la.cid

--Adding a Surrogate key so that we have a Primary Key & Making a View
Create View gold.dim_customers AS
Select
	Row_Number() Over (Order By cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	Case when ci.cst_gndr != 'n/a' Then ci.cst_gndr
		Else COALESCE(ca.gen, 'n/a')
	End as gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
From silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
On ci.cst_key = ca.cid
Left Join silver.erp_loc_a101 as la
On ci.cst_key = la.cid

Select * From gold.dim_customers

--Updating the Gold Layer (prd_info)-----------------------------------------------------------------------------------------------------------
Select * From silver.crm_prd_info

Select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt
From silver.crm_prd_info AS pn
Where prd_end_dt Is Null --Want only current Data,if end_dt is Null that means it is live, so taking only that

--Updating the Gold layer(prd_info)
Select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
From silver.crm_prd_info AS pn
Left Join silver.erp_px_cat_g1v2 AS pc
On pn.cat_id = pc.id
Where prd_end_dt Is Null --Filtering out all historical data

--Checking whether Duplicate data is present
Select prd_key, Count(*) From (
Select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
From silver.crm_prd_info AS pn
Left Join silver.erp_px_cat_g1v2 AS pc
On pn.cat_id = pc.id
Where prd_end_dt Is Null
)t Group By prd_key
Having Count(*) > 1

--Renaming the columns
Select 
	pn.prd_id AS product_id,
	pn.prd_key As product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
From silver.crm_prd_info AS pn
Left Join silver.erp_px_cat_g1v2 AS pc
On pn.cat_id = pc.id
Where prd_end_dt Is Null

--Adding a Surrogate key so that we have a Primary Key & Making a View
Create View gold.dim_products AS
Select 
	Row_Number() Over (Order By pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key As product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
From silver.crm_prd_info AS pn
Left Join silver.erp_px_cat_g1v2 AS pc
On pn.cat_id = pc.id
Where prd_end_dt Is Null

Select * From gold.dim_products

--Updating the Gold Layer (sales_details)-----------------------------------------------------------------------------------------------------------
Select * From silver.crm_sales_details

Select 
	sd.sls_ord_num,
	sd.sls_prd_key,
	sd.sls_cust_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
	From silver.crm_sales_details AS sd

--Updating the Gold Layer (sales_details)
Select 
	sd.sls_ord_num,
	pr.product_key,
	cu.customer_id,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
	From silver.crm_sales_details AS sd
	Left Join gold.dim_products AS pr --Using from View
	ON sd.sls_prd_key = pr.product_number
	Left Join gold.dim_customers AS cu --Using from View
	ON sd.sls_cust_id = cu.customer_id

--Renaming the Columns
Select 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt As shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales As sales_amount,
	sd.sls_quantity As quantity,
	sd.sls_price
	From silver.crm_sales_details AS sd
	Left Join gold.dim_products AS pr
	ON sd.sls_prd_key = pr.product_number
	Left Join gold.dim_customers AS cu
	ON sd.sls_cust_id = cu.customer_id

--Adding a View (Not adding the Surrogate Key as we already mapped from other)
Create View gold.fact_sales As
Select 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_id,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt As shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales As sales_amount,
	sd.sls_quantity As quantity,
	sd.sls_price
	From silver.crm_sales_details AS sd
	Left Join gold.dim_products AS pr
	ON sd.sls_prd_key = pr.product_number
	Left Join gold.dim_customers AS cu
	ON sd.sls_cust_id = cu.customer_id

Select * From gold.fact_sales

--********************************///////////////////*****__|||__*****//////////////////////********************************--

--*******************************************SQL Exploratory Data Analysis*******************************************--

--********************************///////////////////*****__|||__*****//////////////////////********************************--

--Explore All Objects in the Database
Select * From Information_Schema.Tables

--Explore All Columns in the Database
Select * From Information_Schema.Columns

--Explore All Countries our Customers come from
Select Distinct country From gold.dim_customers

--Explore All Categories 
Select Distinct category, subcategory, product_name from gold.dim_products

--// Find the date of the first & last order

Select order_date from gold.fact_sales

Select 
Min(order_date) as first_order_date,
Max(order_date) as last_order_date
From gold.fact_sales

---How many years of sales are available

Select
Min(order_date) as first_order_date,
Max(order_date) as last_order_date,
Datediff(year, Min(order_date), Max(order_date)) as order_range_years
from gold.fact_sales

---How many Months of sales are available

Select
Min(order_date) as first_order_date,
Max(order_date) as last_order_date,
Datediff(month, Min(order_date), Max(order_date)) as order_range_months
from gold.fact_sales

--// Find the Youngest & Oldest Customer

Select
Min(birthdate) as oldest_birthdate,
Max(birthdate) as youngest_birthdate
from gold.dim_customers

---Find the Youngest & Oldest Customer Age

Select
Min(birthdate) as oldest_birthdate,
Datediff(year, Min(birthdate), Getdate()) As oldest_age,
Max(birthdate) as youngest_birthdate,
Datediff(year, Max(birthdate), Getdate()) As youngest_age
from gold.dim_customers

--// Find the Total Sales
Select Sum(sales_amount) as total_sales From gold.fact_sales

--// Find how many items are sold
Select Sum(quantity) as total_quantity From gold.fact_sales

--// Find the average selling price
Select Avg(sls_price) as avg_selling_price From gold.fact_sales

--// Find the Total No. of Orders
Select Count(order_number) as total_orders From gold.fact_sales

Select Count(Distinct order_number) as total_orders From gold.fact_sales

--// Find the Total No. of Products
Select Count(product_key) as total_products From gold.dim_products

Select Count(Distinct product_key) as total_products From gold.dim_products

--// Find the Total No. of Customers
Select Count(customer_id) as total_customers From gold.dim_customers

Select Count(Distinct customer_id) as total_customers From gold.dim_customers

--// Find the Total No. of Customers that has placed an order
Select Count(Distinct customer_id) as total_customers From gold.fact_sales

-- Generate a report that shows all key metrics of the business

Select 'Total Sales' as measure_name, Sum(sales_amount) as measure_value From gold.fact_sales
Union All
Select 'Total Quantity' as measure_name, Sum(quantity) as measure_value From gold.fact_sales
Union All
Select 'Average Price' as measure_name, Avg(sls_price) as measure_value From gold.fact_sales
Union All
Select 'Total Nr. Orders' as measure_name, Count(Distinct order_number) From gold.fact_sales
Union All
Select 'Total Nr. Products' as measure_name, Count(product_name) From gold.dim_products
Union All
Select 'Total Nr. Customers' as measure_name, Count(customer_key) From gold.dim_customers

-- Find total customers by Countries
Select
country,
Count(customer_key) AS total_customers
From gold.dim_customers
Group By country
Order By total_customers Desc

-- Find total customers by Gender
Select 
gender,
Count(customer_key) as total_customers
From gold.dim_customers
Group By gender
Order By total_customers Desc

-- Find total products by category
Select 
category,
Count(product_id) as total_products
From gold.dim_products
Group by category
Order by total_products Desc

-- What is the average costs in each category?
Select
category,
avg(cost) as avg_costs
From gold.dim_products
Group by category
Order by avg_costs Desc

-- What is the total revenue generated for each category?
Select
category,
Sum(cost) as total_revenue
From gold.dim_products
Group By category
Order By total_revenue Desc

Select
p.category,
Sum(f.sales_amount) as total_revenue
From gold.fact_sales as f
Left join gold.dim_products as p
On p.product_key = f.product_key
Group by p.category

-- Find total revenue generated by each customer
Select
customer_id,
Sum(sales_amount) as total_revenue
From gold.fact_sales
Group by customer_id
Order by total_revenue Desc

Select
c.customer_key,
c.first_name,
c.last_name,
Sum(f.sales_amount) as total_revenue
From gold.dim_customers as c
Left Join gold.fact_sales as f
On c.customer_id = f.customer_id
Group by c.customer_key, c.first_name, c.last_name
Order by total_revenue Desc

-- What is the distribution of sold items across countries?
Select
c.country,
Sum(f.quantity) as total_sold_items
From gold.dim_customers as c
Left Join gold.fact_sales as f
On c.customer_id = f.customer_id
Group by c.country
Order by total_sold_items Desc

-- Which 5 products generate the highest revenue?
Select Top 5
product_key,
Sum(sales_amount) as Revenue
From gold.fact_sales
Group By product_key
Order By Revenue Desc;

Select Top 5
product_key,
Sum(sales_amount) as Revenue
From gold.fact_sales
Group by product_key
Order by Revenue Asc;

Select Top 5
p.product_key,
Sum(f.sales_amount) as Revenue
From gold.dim_products as p
Left Join gold.fact_sales as f
On p.product_key = f.product_key
Group by p.product_key
Order by Revenue Desc

-- What are the 5 worst-performing products in terms of sales?
Select Top 5
p.product_name,
Sum(f.sales_amount) as sales
From gold.fact_sales as f
Left join gold.dim_products as p
On p.product_key = f.product_key
Group By p.product_name
Order By sales 

-- Which 5 Subcategory generate the highest revenue?
Select Top 5
p.subcategory,
Sum(f.sales_amount) as revenue
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Group By p.subcategory
Order By revenue Desc

-- What are the 5 worst-performing Subcategory in terms of sales?
Select Top 5
p.subcategory,
Sum(f.sales_amount) as revenue
From gold.dim_products as p
Left Join gold.fact_sales as f
On p.product_key = f.product_key
Group By p.subcategory
Order By revenue Asc

-- Which 5 products generate the highest revenue in Report Format?
Select * From
(Select
	p.product_name,
	Sum(f.sales_amount) as revenue,
	Row_Number() Over (Order By Sum(f.sales_amount) Desc) as rank_products
	From gold.dim_products as p
	Left Join gold.fact_sales as f
	On p.product_key = f.product_key
	Group By p.product_name)t
Where rank_products <=5

-- Find the top 10 customers who have generated the highest revenue
Select Top 10
c.customer_id,
c.first_name,
c.last_name,
Sum(f.sales_amount) as revenue
From gold.dim_customers as c
Left Join gold.fact_sales as f
On c.customer_id = f.customer_id
Group By c.customer_id,c.first_name,c.last_name
Order By revenue Desc

-- Find the top 10 customers who have generated the highest revenue in Report Format
Select * From
(Select
	c.customer_id,
	c.first_name,
	c.last_name,
	Sum(f.sales_amount) as revenue,
	Row_number () Over (Order By Sum(f.sales_amount) Desc) as rank_products
	From gold.dim_customers as c
	Left Join gold.fact_sales as f
	On c.customer_id = f.customer_id
	Group By c.customer_id, c.first_name, c.last_name)t
Where rank_products <=10

-- The 3 customers with the fewest orders placed
Select Top 3
c.customer_id,
c.first_name,
c.last_name,
Count(Distinct f.order_number) as Orders
From gold.dim_customers as c
Left Join gold.fact_sales as f
On c.customer_id = f.customer_id
Group By c.customer_id,c.first_name,c.last_name
Order By Orders Asc

-- The 3 customers with the fewest orders placed in Report Format
Select * From
(Select 
	c.customer_id, 
	c.first_name,
	c.last_name,
	Count(Distinct f.order_number) as Orders,
	Row_Number () Over(Order By Count(Distinct f.order_number) Asc) as rank_orders
	From gold.dim_customers as c
	Left Join gold.fact_sales as f
	On c.customer_id = f.customer_id
	Group By c.customer_id,c.first_name,c.last_name)t
Where rank_orders <=3

--********************************///////////////////*****__|||__*****//////////////////////********************************--

--*******************************************SQL Advance Analytics*******************************************--

--********************************///////////////////*****__|||__*****//////////////////////********************************--

-- Sales Performance Over Years
Select 
order_date,
Sum(sales_amount) as sales
From gold.fact_sales
Where order_date IS Not Null
Group By order_date 
Order By order_date 

Select
Year(order_date) as order_year,
Sum(sales_amount) as sales
From gold.fact_sales
Where Year(order_date) IS Not Null
Group By Year(order_date) 
Order By order_year

-- Total Customers Over Years
Select
Year(order_date) as order_year,
Sum(sales_amount) as sales,
Count(Distinct customer_id) as total_customer,
Sum(quantity) as quantity
From gold.fact_sales
Where Year(order_date) Is Not Null
Group By Year(order_date) 
Order By total_customer

-- Total Customers Over Months
Select 
Month(order_date) as order_month,
Sum(sales_amount) as sales,
Count(Distinct customer_id) as total_customer,
Sum(quantity) as quantity
From gold.fact_sales
Where Month(order_date) Is Not Null
Group By Month(order_date) 
Order By order_month

-- Total Customers Over Years & Months
Select
Format(order_date, 'yyyy-MMM') as order_date,
Sum(sales_amount) as sales,
Count(Distinct customer_id) as total_customer,
Sum(quantity) as quantity
From gold.fact_sales
Where Format(order_date,'yyyy-MMM') Is Not Null
Group By Format(order_date, 'yyyy-MMM')
Order By order_date

-- Calculate the total Sales per month & the running total of sales over time
Select
order_date,sales,
Sum(sales) Over (Order By order_date) as running_total
From
(Select
Format(order_date, 'yyyy-MMM') as order_date,
Sum(sales_amount) as sales,
Count(Distinct customer_id) as total_customers,
Sum(quantity) as quantity
From gold.fact_sales
Group By Format(order_date, 'yyyy-MMM') )t

Select 
order_date,total_sales,
Sum(total_sales) Over (Order By order_date) As running_total_sales
From 
( Select
Datetrunc(month, order_date) as order_date,
Sum(sales_amount) as total_sales
From gold.fact_sales
Where order_date Is Not Null
Group By Datetrunc(month, order_date) )t

-- Calculate the total Sales per year & the running total of sales over time
Select
order_date,total_sales,
Sum(total_sales) Over(Order By order_date) As running_total_sales
From
(Select
Datetrunc(Year,order_date) as order_date,
Sum(sales_amount) as total_sales
From gold.fact_sales
Where order_date Is Not Null
Group By Datetrunc(Year,order_date))t

-- Calculate the total Sales per year,the running total of sales over time & moving avg price
Select
order_date,total_sales,
Sum(total_sales) Over(Order By order_date) as running_total_sales,
Avg(avg_price) Over(Order By order_date) as moving_avg_price
From
(Select
Datetrunc(Year,order_date) as order_date,
Sum(sales_amount) as total_sales,
Avg(sls_price) as avg_price
From gold.fact_sales
Where order_date Is Not Null
Group By Datetrunc(Year,order_date) )t

-- Analyze the yearly performance of products by comparing each product's sales to both its avg sales performance & previous year's sales (Report Format)
Select
order_year,
current_sales,
Avg(current_sales) Over (Partition By product_name) as avg_sales,
current_sales - Avg(current_sales) Over (Partition By product_name) as diff_avg,
Case When current_sales - Avg(current_sales) Over (Partition By product_name) > 0 Then 'Above avg'
	When current_sales - Avg(current_sales) Over (Partition By product_name) < 0 Then 'Below avg'
	Else 'Avg'
End avg_change
From
(Select
Year(f.order_date) as order_year,
p.product_name,
Sum(f.sales_amount) as current_sales
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Where order_date Is Not Null
Group By Year(f.order_date), p.product_name)t   -- Can't sort by Year, so report method is not working will create a cte 

-- Analyze the yearly performance of products by comparing each product's sales to both its avg sales performance & previous year's sales (CTE Format)
With yearly_product_sales As 
( Select 
Year(f.order_date) As order_year,
Sum(f.sales_amount) As current_sales,
p.product_name
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Where f.order_date Is Not Null
Group By Year(f.order_date),p.product_name)

Select 
order_year,product_name,current_sales,
Avg(current_sales) Over (Partition By product_name) as avg_sales,
current_sales - Avg(current_sales) Over (Partition By product_name) as diff_avg,
Case When current_sales - Avg(current_sales) Over (Partition By product_name) > 0 Then 'Above Avg'
	 When current_sales - Avg(current_sales) Over (Partition By product_name) < 0 Then 'Below Avg'
	 Else 'Avg'
End avg_change,
Lag(current_sales) Over (Partition By product_name Order By order_year) as py_sales,
current_sales - Lag(current_sales) Over (Partition By product_name Order By order_year) as diff_py,
Case When current_sales - Lag(current_sales) Over (Partition By product_name Order By order_year) > 0 Then 'Increase'
	 When current_sales - Lag(current_sales) Over (Partition By product_name Order By order_year) < 0 Then 'Decrease'
	 Else 'No Change'
End py_change
From yearly_product_sales
Order By product_name, order_year 

-- Analyze the Monthly performance of products by comparing each product's sales to both its avg sales performance & previous Month's sales (CTE Format)
With monthly_product_sales As 
( Select 
Month(f.order_date) as order_month,
p.product_name,
Sum(f.sales_amount) as current_sales
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Where f.order_date is Not Null
Group By Month(f.order_date),p.product_name)

Select 
order_month,product_name,current_sales,
Avg(current_sales) Over (Partition By product_name) as avg_sales,
current_sales - Avg(current_sales) Over (Partition By product_name) as diff_avg,
Case When current_sales - Avg(current_sales) Over (Partition By product_name) > 0 Then 'Above Avg'
	 When current_sales - Avg(current_sales) Over (Partition By product_name) < 0 Then 'Below Avg'
	 Else 'Avg'
End as avg_change,
Lag(current_sales) Over (Partition By product_name Order By order_month) as pm_sales,
current_sales - Lag(current_sales) Over (Partition By product_name Order By order_month) as diff_pm,
Case When current_sales - Lag(current_sales) Over (Partition By product_name Order By order_month) > 0 Then 'Increase'
	 When current_sales - Lag(current_sales) Over (Partition By product_name Order By order_month) < 0 Then 'Decrease'
	 Else 'No Change'
End as py_change
From monthly_product_sales
Order By product_name, order_month

-- Which Categories contribute the most to overall sales (Report Format)
Select category,
total_sales,
Sum(total_sales) Over () as overall_sales,
Concat(Round((Cast (total_sales As Float) / Sum(total_sales) Over ()) *100,2),'%') as percentage_of_total
From
(Select 
category,
Sum(sales_amount) as total_sales
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Group by category)t

-- Which Categories contribute the most to overall sales (CTE Format)
With category_sales As (
Select
category,
Sum(sales_amount) as total_sales
From gold.fact_sales as f
Left Join gold.dim_products as p
On p.product_key = f.product_key
Group by category )

Select
category,total_sales,
Sum(total_sales) Over () as overall_sales,
Concat(Round((Cast (total_sales As Float) / Sum(total_sales) Over ()) *100,2),'%') as percentage_of_total
From category_sales
Order By percentage_of_total

-- Which Customer contribute the most to overall sales (CTE Format)
With customer_sales As (
Select
c.customer_key,
c.first_name,
c.last_name,
Sum(f.sales_amount) as total_sales
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Group By c.customer_key, c.first_name, c.last_name ) 

Select
customer_key,first_name,last_name,total_sales,
Sum(total_sales) Over () as overall_sales,
Concat(Round(( Cast (total_sales as Float) / Sum(total_sales) Over ()) *100, 2),'%') as percentage_of_total
from customer_sales
Order By percentage_of_total

-- Segment products into cost ranges & count how many products fall onto each segment.
With product_segments As
( Select 
product_key,product_name,cost,
Case When cost < 100 Then 'Below 100'
	 When cost Between 100 And 500 Then '100-500'
	 When cost Between 500 And 1000 Then '500-1000'
	 Else 'Above 1000'
End As cost_range
From gold.dim_products )

Select
cost_range, 
Count(product_key) as total_products
From product_segments
Group By cost_range

/* Group customers into 3 segments based on their spending behavior:
	- VIP: Customers with at least 12 Months of history & spending more than 5,000.
	- Regular: Customers with at lease 12 Months of history but spending 5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total no. of customers by each group */

With customer_spending As 
( Select 
c.customer_id,
Sum(f.sales_amount) as total_spending,
Min(f.order_date) as first_order,
Max(f.order_date) as last_order,
Datediff (month, Min(f.order_date), Max(f.order_date)) as lifespan
From gold.dim_customers as c
Left Join gold.fact_sales as f
On c.customer_id = f.customer_id
Group By c.customer_id )

Select 
customer_id,
total_spending,
lifespan,
Case When lifespan >= 12 And total_spending > 5000 Then 'VIP'
	 When lifespan >= 12 And total_spending <= 5000 Then 'Regular'
	 Else 'New'
End As customer_segment
From customer_spending

-- Finding the total no. of customers by each group
With customer_spending As 
( Select
c.customer_id,
Sum(f.sales_amount) as total_spending,
Min(f.order_date) as first_order,
Max(f.order_date) as last_order,
Datediff (Month, Min(f.order_date), Max(f.order_date)) as lifespan
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Group By c.customer_id )

Select
Case When lifespan >= 12 And total_spending > 5000 Then 'VIP'
	 When lifespan >= 12 And total_spending <= 5000 Then 'Regular'
	 Else 'New'
End customer_segment,
Count(customer_id) as total_customers
From customer_spending
Group By
Case When lifespan >= 12 And total_spending > 5000 Then 'VIP'
	 When lifespan >= 12 And total_spending <= 5000 Then 'Regular'
	 Else 'New'
End

/*
=====================================================================================================================
Customer Report
=====================================================================================================================
Purpose:
	- This report consolidates key customer metrics & behaviors
Highlights:
	1. Gathers essential fields such as names,ages & transaction details.
	2. Segments customers into categories (VIP, Regular, New) & age groups.
	3. Aggregates customer-level metrics:
		- total orders, total sales, total quantity purchased, total products, lifespan(in Months)
	4. Calculate valuable KPIs:
		- recency(Months since last order), avg order value, avg monthly spend
=====================================================================================================================
*/

-- Base querry
Select
f.order_number,f.product_key,
f.order_date,f.sales_amount,
f.quantity,
c.customer_key, c.customer_number,
c.first_name, c.last_name,
c.birthdate
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Where order_date is Not Null

With base_querry As
( Select
f.order_number,f.product_key,
f.order_date,f.sales_amount,
f.quantity,
c.customer_key, c.customer_number,
Concat(c.first_name, ' ', c.last_name) as customer_name,
Datediff(Year, c.birthdate, Getdate()) age
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Where order_date is Not Null )

Select order_number,product_key,
order_date,sales_amount,
quantity, customer_name, age
From base_querry

-- Aggregation
With base_querry As
( Select
f.order_number,f.product_key,
f.order_date,f.sales_amount,
f.quantity,
c.customer_key, c.customer_number,
Concat(c.first_name, ' ', c.last_name) as customer_name,
Datediff(Year, c.birthdate, Getdate()) age
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Where order_date is Not Null )

Select
customer_key,customer_number,
customer_name,age,
Count(Distinct order_number) as total_orders,
Sum(sales_amount) as total_sales,
Sum(quantity) as total_quantity,
Count(Distinct product_key) as total_products,
Max(order_date) as last_order_date,
Datediff(Month, Min(order_date), Max(order_date)) as lifespan
From base_querry
Group By
customer_key,customer_number,
customer_name, age

-------------------------------------------Customer metrics & Behaviors Report building -------------------------------------------
Create View gold.report_customers As
With base_querry As
-----------Base_querry---------------------
( Select
f.order_number,f.product_key,
f.order_date,f.sales_amount,
f.quantity,
c.customer_key, c.customer_number,
Concat(c.first_name, ' ', c.last_name) as customer_name,
Datediff(Year, c.birthdate, Getdate()) age
From gold.fact_sales as f
Left Join gold.dim_customers as c
On c.customer_id = f.customer_id
Where order_date is Not Null )

, customer_aggregation As (
-----------Aggregation---------------
Select
customer_key,customer_number,
customer_name,age,
Count(Distinct order_number) as total_orders,
Sum(sales_amount) as total_sales,
Sum(quantity) as total_quantity,
Count(Distinct product_key) as total_products,
Max(order_date) as last_order_date,
Datediff(Month, Min(order_date), Max(order_date)) as lifespan
From base_querry
Group By
customer_key,customer_number,
customer_name, age
)

Select
customer_key,customer_number,
customer_name,age,
Case When age < 20 Then 'Under 20'
	 When age between 20 and 29 Then '20-29'
	 When age between 30 and 39 Then '30-39'
	 When age between 40 and 49 Then '40-49'
	 Else '50 and above'
End As age_group,
Case When lifespan >= 12 And total_sales > 5000 Then 'VIP'
	 When lifespan >= 12 And total_sales <= 5000 Then 'Regular'
	 Else 'New'
End As customer_segment,
last_order_date,
Datediff(Month, last_order_date, Getdate()) as recency,
total_orders,total_sales,
total_quantity,total_products,
lifespan,
-- Compute avg order value
Case When total_sales = 0 Then 0
	Else total_sales / total_orders
End As avg_order_value,
-- Compute avg monthly spend
Case When lifespan = 0 then total_sales
	Else total_sales / lifespan
End As avg_monthly_spend
From customer_aggregation

--**********************************=====================***********************************--
Select * from gold.report_customers
--**********************************=====================***********************************--

Select 
age_group,
Count(customer_number) as total_customers,
Sum(total_sales) as total_sales
From gold.report_customers
Group By age_group

Select 
customer_segment,
Count(customer_number) as total_customers,
Sum(total_sales) as total_sales
From gold.report_customers
Group By customer_segment

/*
=====================================================================================================================
Product Report
=====================================================================================================================
Purpose:
	- This report consolidates key product metrics & behaviors
Highlights:
	1. Gathers essential fields such as product name, category, subcategory & cost.
	2. Segments products by revenue to identify High performers, Mid & Low performers.
	3. Aggregates customer-level metrics:
		- total orders, total sales, total quantity sold, total customers (unique), lifespan(in Months)
	4. Calculate valuable KPIs:
		- recency(Months since last sale), avg order revenue, avg monthly revenue
=====================================================================================================================
*/

Create View gold.report_products As
With base_query as (
-- Base querry
Select
f.order_number,f.order_date,
f.sales_amount,f.quantity,f.customer_id,
p.product_key,p.product_name,
p.category,p.subcategory,
p.cost
From gold.fact_sales as f
Left Join gold.dim_products as p
On f.product_key = p.product_key 
Where order_date Is Not Null
),

product_aggregations As (
-- Product aggregations
Select
product_key,product_name,
category,subcategory,
cost,
Datediff(Month, Min(order_date), Max(order_date)) as lifespan,
Max(order_date) as last_sale_date,
Count(Distinct order_number) as total_orders,
Count(Distinct customer_id) as total_customers,
Sum(sales_amount) as total_sales,
Sum(quantity) as total_quantity,
Round(Avg(Cast(sales_amount as Float) / Nullif(quantity,0)),1) as avg_selling_price
From base_query
Group By
product_key,product_name,
category,subcategory,cost
)

--  Final querry
Select
product_key,product_name,
category,subcategory,
cost,last_sale_date,
Datediff(Month, last_sale_date, Getdate()) as recency_in_months,
Case
	When total_sales > 50000 Then 'High-Performer'
	When total_sales >= 10000 Then 'Mid-Range'
	Else 'Low-Performer'
End As product_segment,
lifespan,total_orders,
total_sales,total_quantity,
total_customers,avg_selling_price,
Case
	When total_orders = 0 Then 0
	Else total_sales / total_orders
End As avg_order_revenue,
Case
	When lifespan = 0 Then total_sales
	Else total_sales / lifespan
End As avg_monthly_revenue
From product_aggregations

--**********************************=====================***********************************--
Select * from gold.report_products
--**********************************=====================***********************************--
