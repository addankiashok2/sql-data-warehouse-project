/*
=================================================================
Create Database and Schema
=================================================================
Script Purpose:
	This script create a new Database name 'DataWarehouse' after checking if it already exists.
	If the databse exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within the databse: 'bronze', 'silver', 'gold'.

WARNING:
	Running this script will remove the 'Datawarehouse' Database if it there and data also will be deleted
	permanantly. Proceed with caution and take a backup before running the script
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
-- Create the schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;

