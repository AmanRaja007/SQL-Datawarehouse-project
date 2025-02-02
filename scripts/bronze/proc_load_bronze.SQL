/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `Copy` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call bronze.load_bronze();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP := CURRENT_TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
	RAISE NOTICE '==============================';
	RAISE NOTICE 'LOADING BRONZE LAYER';
	RAISE NOTICE '==============================';
	RAISE NOTICE '--------------------------------';
	RAISE NOTICE 'LOADING CRM DETAILS';
	RAISE NOTICE '----------------------------------';

  -- Truncate crm_cust_info table and load data
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.crm_cust_info';
  EXECUTE 'TRUNCATE TABLE bronze.crm_cust_info';

	RAISE NOTICE '>> INSERTING DATA INTO: bronze.crm_cust_info';
    EXECUTE 'COPY bronze.crm_cust_info FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';

    
	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
	-- Truncate crm_prd_info table and load data
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.crm_prd_info';
    EXECUTE 'TRUNCATE TABLE bronze.crm_prd_info';
	RAISE NOTICE '>> INSERTING DATA INTO: bronze.crm_prd_info';
    EXECUTE 'COPY bronze.crm_prd_info FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';

	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    
	-- Truncate crm_sales_details table and load data
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.crm_sales_details';
    EXECUTE 'TRUNCATE TABLE bronze.crm_sales_details';
	RAISE NOTICE '>> INSERTING DATA INTO: bronze.crm_sales_details';
    EXECUTE 'COPY bronze.crm_sales_details FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';
	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	 -- Truncate erp_cust_az12 table and load data
	RAISE NOTICE '==============================';
	RAISE NOTICE 'LOADING BRONZE LAYER';
	RAISE NOTICE '==============================';
	RAISE NOTICE '--------------------------------';
	RAISE NOTICE 'LOADING ERP DETAILS';
	RAISE NOTICE '----------------------------------';
	
   
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.erp_cust_az12';
    EXECUTE 'TRUNCATE TABLE bronze.erp_cust_az12';
	RAISE NOTICE '>> INSERTING DATA INTO: bronze.erp_cust_az12';
    EXECUTE 'COPY bronze.erp_cust_az12 FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';
	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
    -- Truncate erp_loc_a101 table and load data
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.erp_loc_a101';
    EXECUTE 'TRUNCATE TABLE bronze.erp_loc_a101';
	RAISE NOTICE '>> INSERTING DATA INTO: bronze.erp_loc_a101';
    EXECUTE 'COPY bronze.erp_loc_a101 FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';
	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
    -- Truncate erp_px_cat_g1v2 table and load data
	START_TIME := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
    EXECUTE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
	RAISE NOTICE '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
    EXECUTE 'COPY bronze.erp_px_cat_g1v2 FROM ''D:/MY work/SQl Project (Date Warehouse)/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'' WITH (FORMAT csv, HEADER, DELIMITER '','')';
	end_time = current_timestamp;
	RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
	
    -- Optionally, you can add SELECT count(*) to check the data loaded
    -- But SELECT count(*) can't be executed directly in PL/pgSQL as it needs to be fetched into a variable.
    -- Example: EXECUTE 'SELECT count(*) FROM bronze.crm_cust_info' INTO my_variable;

END;
$$;
