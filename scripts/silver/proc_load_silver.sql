/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver();
===============================================================================
*/
-- Stored Procedure 
CREATE 
OR REPLACE PROCEDURE silver.load_silver() LANGUAGE plpgsql AS $$ DECLARE start_time TIMESTAMP;
end_time TIMESTAMP;
batch_start_time TIMESTAMP := CURRENT_TIMESTAMP;
batch_end_time TIMESTAMP;
BEGIN -- Truncate crm_cust_info table and load data
RAISE NOTICE '==============================';
RAISE NOTICE 'LOADING SILVER LAYER';
RAISE NOTICE '==============================';
RAISE NOTICE '--------------------------------';
RAISE NOTICE 'LOADING CRM DETAILS';
RAISE NOTICE '----------------------------------';
RAISE NOTICE 'Truncating Table: silver.crm_cust_info';
START_TIME := CURRENT_TIMESTAMP;
-- Truncate table
TRUNCATE TABLE silver.crm_cust_info;
RAISE NOTICE 'Inserting Data into: silver.crm_cust_info';
-- Insert new data
INSERT INTO silver.crm_cust_info (
  cst_id, cst_key, cst_firstname, cst_lastname, 
  cst_marital_status, cst_gndr, cst_create_date
) 
SELECT 
  cst_id, 
  cust_key, 
  TRIM(cst_firstname), 
  TRIM(cst_lastname), 
  cst_marital_status, 
  CASE WHEN UPPER(
    TRIM(cst_gndr)
  ) = 'F' THEN 'Female' WHEN UPPER(
    TRIM(cst_gndr)
  ) = 'M' THEN 'Male' ELSE 'n/a' END AS cst_gndr, 
  -- Normalize gender values to readable format
  cst_create_date 
FROM 
  (
    SELECT 
      *, 
      ROW_NUMBER() OVER (
        PARTITION BY cst_id 
        ORDER BY 
          cst_create_date DESC
      ) AS flag_last 
    FROM 
      bronze.crm_cust_info 
    WHERE 
      cst_id IS NOT NULL
  ) AS t 
WHERE 
  flag_last = 1;
-- Select the most recent record per customer
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
START_TIME = CURRENT_TIMESTAMP;
-- Build silver Layer clean & load crm_prd_info
RAISE NOTICE 'Truncating Table: silver.crm_prd_info';
-- Truncate table
TRUNCATE TABLE silver.crm_prd_info;
RAISE NOTICE 'Inserting Data into: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info(
  prd_id, cat_id, prd_key, prd_nm, prd_cost, 
  prd_line, prd_start_dt, prd_end_dt
) 
select 
  prd_id, 
  Replace(
    substring(prd_key, 1, 5), 
    '-', 
    '_'
  ) as cat_id, 
  -- Extract category ID 
  SUBSTRING(
    PRD_KEY, 
    7, 
    LENGTH(PRD_KEY)
  ) AS prd_key, 
  -- Extract product ID 
  prd_nm, 
  coalesce(prd_cost, 0) as prd_cost, 
  case upper(
    trim(prd_line)
  ) when 'M' then 'Mountain' when 'R' then 'Road' when 'S' then 'Other Sales' when 'T' then 'Touring' else 'n/a' end as prd_line, 
  -- Map product line codes to descriptive values
  cast(prd_start_dt as Date) as prd_start_dt, 
  lead(prd_start_dt) over(
    partition by prd_key 
    order by 
      prd_start_dt
  )- interval '1 day' as prd_end_dt -- Calculate end date as one day before the next start date
from 
  bronze.crm_prd_info;
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
START_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE 'Truncating Table: silver.crm_sales_details';
-- Truncate table
TRUNCATE TABLE silver.crm_sales_details;
RAISE NOTICE 'Inserting Data into: silver.crm_sales_details';
-- insert new data to  silver.crm_sales_details
INSERT INTO silver.crm_sales_details(
  sls_ord_num, sls_prd_key, sls_cst_id, 
  sls_order_dt, sls_ship_dt, sls_due_dt, 
  sls_sales, sls_quantity, sls_price
) 
select 
  sls_ord_num, 
  sls_prd_key, 
  sls_cst_id, 
  CASE WHEN sls_order_dt = 0 
  OR LENGTH(
    CAST(sls_order_dt AS TEXT)
  ) != 8 THEN NULL ELSE TO_DATE(
    CAST(sls_order_dt AS TEXT), 
    'YYYYMMDD'
  ) END AS sls_order_dt, 
  CASE WHEN sls_ship_dt = 0 
  OR LENGTH(
    CAST(sls_ship_dt AS TEXT)
  ) != 8 THEN NULL ELSE TO_DATE(
    CAST(sls_ship_dt AS TEXT), 
    'YYYYMMDD'
  ) END AS sls_ship_dt, 
  CASE WHEN sls_due_dt = 0 
  OR LENGTH(
    CAST(sls_due_dt AS TEXT)
  ) != 8 THEN NULL ELSE TO_DATE(
    CAST(sls_due_dt AS TEXT), 
    'YYYYMMDD'
  ) END AS sls_due_dt, 
  CASE WHEN sls_sales IS NULL 
  OR sls_sales <= 0 
  OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales, 
  sls_quantity, 
  CASE WHEN sls_price IS NULL 
  OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) ELSE sls_price END AS sls_price 
from 
  bronze.crm_sales_details;
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
RAISE NOTICE '==============================';
RAISE NOTICE 'LOADING SILVER LAYER';
RAISE NOTICE '==============================';
RAISE NOTICE '--------------------------------';
RAISE NOTICE 'LOADING ERP DETAILS';
RAISE NOTICE '----------------------------------';
RAISE NOTICE '---------------------';
RAISE NOTICE 'Truncating Table: silver.erp_cust_az12';
START_TIME = CURRENT_TIMESTAMP;
-- Truncate table
TRUNCATE TABLE silver.erp_cust_az12;
RAISE NOTICE 'Inserting Data into: silver.erp_cust_az12';
-- insert new data to  silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12(cid, bdate, gen) 
select 
  CASE WHEN cid like 'NAS%' THEN SUBSTRING(
    cid, 
    4, 
    length(cid)
  ) ELSE cid END AS cid, 
  -- Remove 'NAS' Prefix if present
  CASE WHEN bdate > NOW() THEN NULL ELSE bdate END AS bdate, 
  -- Set future birthdates to NULL
  CASE WHEN UPPER(
    TRIM(gen)
  ) IN ('F', 'FEMALE') THEN 'Female' WHEN UPPER(
    TRIM(gen)
  ) IN ('M', 'MALE') THEN 'Male' ELSE 'n/a' END AS gen -- Normalize gender values and handle unkown cases
FROM 
  bronze.erp_cust_az12;
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
RAISE NOTICE 'Truncating Table: silver.erp_loc_a101';
START_TIME = CURRENT_TIMESTAMP;
-- Truncate table
TRUNCATE TABLE silver.erp_loc_a101;
RAISE NOTICE 'Inserting Data into: silver.erp_loc_a101';
-- insert new data to  silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101(cid, cntry) 
select 
  REPLACE(cid, '-', '') as cid, 
  CASE WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States' WHEN TRIM(cntry) = '' 
  OR cntry IS NULL THEN 'n/a' WHEN TRIM(cntry) = 'DE' THEN 'Germany' ELSE TRIM(CNTRY) END AS cntry --NORMALIZE AND HANDLE MISSING OR BLANK COUNTRY CODE 
from 
  bronze.erp_loc_a101;
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
START_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE 'Truncating Table: silver.erp_px_cat_g1v2';
-- Truncate table
TRUNCATE TABLE silver.erp_px_cat_g1v2;
RAISE NOTICE 'Inserting Data into: silver.erp_px_cat_g1v2';
-- insert new data to  silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance) 
select 
  id, 
  cat, 
  subcat, 
  maintenance 
from 
  bronze.erp_px_cat_g1v2;
END_TIME = CURRENT_TIMESTAMP;
RAISE NOTICE '>>LOAD DURATION: % seconds ', 
EXTRACT(
  EPOCH 
  FROM 
    (END_TIME - START_TIME)
);
END $$;
-- TO EXECUTE USE THE BELOW CODE;
--CALL silver.load_silver()
