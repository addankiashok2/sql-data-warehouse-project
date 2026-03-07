/*
=====================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================================
Script Purpose:
  This stored procedure will loda the .csv files into bronze schema tables as bulk insert
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses 'BULK INSERT' command to loa data from csv files to bronze tables
Parameters:
  None
  This stored procedure doesn't accept any parameters or return any values
Usage example:
EXEC bronze.load_bronze
=====================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME, 
			@end_time DATETIME, 
			@starting_broze_layer DATETIME, 
			@ending_bronze_layer DATETIME;
	BEGIN TRY
		PRINT '=================================================';
		PRINT 'Loading The Broze Layer';
		PRINT '=================================================';

		SET @starting_broze_layer = GETDATE()
		PRINT 'Bronze Layer Execution Started'
		SET @start_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\DataEngineering\sql-ultimate-course\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Loadint Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'Seconds';
		SET @ending_bronze_layer = GETDATE()
		PRINT 'Bronze Layer Execution Ended'
		PRINT '>> Bronze Layer Duration: ' + CAST(DATEDIFF(second, @starting_broze_layer, @ending_bronze_layer) as NVARCHAR) + 'Seconds';

	END TRY
	BEGIN CATCH
		PRINT '===========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Erro Message' + ERROR_MESSAGE();
		PRINT 'Erro Message' + CAST(ERROR_MESSAGE() as NVARCHAR)
	END CATCH
END
