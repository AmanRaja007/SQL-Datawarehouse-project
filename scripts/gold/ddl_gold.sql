/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
DROP 
  VIEW IF EXISTS gold.dim_customers CASCADE;
CREATE VIEW gold.dim_customers AS 
select 
  row_number() over(
    order by 
      cst_id
  ) as customer_key, 
  ci.cst_id as customer_id, 
  ci.cst_key as customer_number, 
  ci.cst_firstname as firstname, 
  ci.cst_lastname as lastname, 
  la.cntry as country, 
  ci.cst_marital_status as marital_status, 
  CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr --Crm is the master table 
  ELSE coalesce(ca.gen, 'n/a') end as gender, 
  ca.bdate as birthdate, 
  ci.cst_create_date as create_date 
from 
  silver.crm_cust_info as ci 
  left join silver.erp_cust_az12 as ca on ci.cst_key = ca.cid 
  left join silver.erp_loc_a101 as la on ci.cst_key = la.cid;
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
DROP 
  VIEW IF EXISTS gold.dim_products CASCADE;
create view gold.dim_products AS 
select 
  row_number() over(
    order by 
      pi.prd_start_dt, 
      pi.prd_key
  ) AS product_key, 
  pi.prd_id as product_id, 
  pi.prd_key as product_num, 
  pi.prd_nm as product_name, 
  pi.cat_id as category_id, 
  pc.cat as category, 
  pc.subcat as subcategory, 
  pc.maintenance as maintenance, 
  pi.prd_cost as cost, 
  pi.prd_line as product_line, 
  pi.prd_start_dt as start_date 
from 
  silver.crm_prd_info as pi 
  left join silver.erp_px_cat_g1v2 as pc on pi.cat_id = pc.id 
where 
  prd_end_dt IS NULL;
-- FILTER OUT THE HISTORICAL DATA
-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================
DROP 
  VIEW IF EXISTS gold.fact_sales;
create view gold.fact_sales AS 
select 
  sd.sls_ord_num as order_number, 
  pr.product_key, 
  cu.customer_key, 
  sd.sls_order_dt as order_date, 
  sd.sls_ship_dt as shipping_date, 
  sd.sls_due_dt as due_date, 
  sd.sls_sales as sales_amount, 
  sd.sls_quantity as quantity, 
  sd.sls_price as price 
from 
  silver.crm_sales_details as sd 
  left join gold.dim_products as pr on sd.sls_prd_key = pr.product_num 
  left join gold.dim_customers as cu on sd.sls_cst_id = cu.customer_id
