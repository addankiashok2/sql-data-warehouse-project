/*
=====================================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
=====================================================================================
Script Purpose:
  This stored procedure will loda the .csv files into Silver schema tables as bulk insert
  It performs the following actions:
  - Truncates the Silver tables before loading data.
  - Uses 'BULK INSERT' command to loa data from csv files to Silver tables
Parameters:
  None
  This stored procedure doesn't accept any parameters or return any values
Usage example:
EXEC Silver.load_Silver
=====================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '>> Truncating Table silver.crm_cust_info...';
		TRUNCATE TABLE [silver].[crm_cust_info];
		PRINT '>> Inserting data into silver.crm_cust_info...';
		SET @start_time = GETDATE();
		PRINT 'SILVER Layer Execution Started';
		INSERT INTO [silver].[crm_cust_info] (
			cst_id,
			cst_key,
			cst_fisrname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_fisrname) cst_fisrname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'NA'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'NA'
			END cst_gndr,
			cst_create_date
		FROM 
			(
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) as Rnk
			FROM [bronze].[crm_cust_info]
			WHERE cst_id IS NOT NULL
			)t
		WHERE Rnk=1
		PRINT '>> Inserting data into silver.crm_cust_info completed';

		PRINT '>> Truncating Table silver.crm_prd_info...';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data into silver.crm_prd_info...';
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
			REPLACE(SUBSTRING(prd_key,1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'NA'
			END prd_line,
			CAST(prd_start_dt as DATE) prd_start_dt,
			CAST(
				LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as DATE) prd_end_dt
		FROM [bronze].[crm_prd_info]
		PRINT '>> Inserting data into silver.crm_prd_info completed';

		PRINT '>> Truncating Table silver.crm_sales_details...';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into silver.crm_sales_details...';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price<=0
				THEN sls_sales/NULLIF(sls_quantity, 0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details
		PRINT '>> Inserting data into silver.crm_sales_details completed';

		PRINT '>> Truncating Table silver.erp_cust_az12...';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into silver.erp_cust_az12...';
		INSERT INTO [silver].[erp_cust_az12](cid, bdate, gen)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				ELSE cid
			END cid,
			CASE WHEN bdate>GETDATE() THEN NULL
				 ELSE bdate
			END bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				 ELSE 'NA'
			END gen
		FROM [bronze].[erp_cust_az12]
		PRINT '>> Inserting data into silver.erp_cust_az12 completed';

		PRINT '>> Truncating Table silver.erp_loc_a101...';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into silver.erp_loc_a101...';
		INSERT INTO [silver].[erp_loc_a101](cid,cntry)
		SELECT 
			REPLACE(cid,'-', '') cid,
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'NA'
				 ELSE TRIM(cntry)
			END cntry
		FROM [bronze].[erp_loc_a101]
		PRINT '>> Inserting data into silver.erp_loc_a101 completed';

		PRINT '>> Truncating Table silver.erp_px_cat_g1v2...';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into silver.erp_px_cat_g1v2...';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT * FROM bronze.erp_px_cat_g1v2
		PRINT '>> Inserting data into silver.erp_px_cat_g1v2 completed';
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
	END TRY
	BEGIN CATCH
		PRINT '===========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Erro Message' + ERROR_MESSAGE();
		PRINT 'Erro Message' + CAST(ERROR_MESSAGE() as NVARCHAR)
	END CATCH
END

