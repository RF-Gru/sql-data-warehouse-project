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

CREATE VIEW gold.dim_customers2 AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key, -- Surrogate key
	customer_id,
	customer_number,
	first_name,
	last_name,
	country,
	marital_status,
	gender,
	birthdate,
	create_date
FROM (

WITH cte2 as
(SELECT 
	ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    loc.cntry AS country,
    ci.cst_marital_status AS marital_status, 
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the Master for gender info
		ELSE COALESCE(ca.gen, 'n/a')
    END as gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date,
    ROW_NUMBER()
	OVER (PARTITION BY cst_id ORDER BY cst_firstname) as flag_last -- got rid of duplicates
 FROM silver.crm_cust_info AS ci
 LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 loc
	ON ci.cst_key = loc.cid)

SELECT *
FROM cte2
WHERE flag_last = 1 )t
;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

CREATE VIEW gold.dim_products2 AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cp.prd_start_dt, cp.prd_key) AS product_key,
	cp.prd_id AS product_id,
    cp.prd_key AS product_number,
    cp.prd_nm AS product_name,
    cp.cat_id AS category_id,
    ep.cat AS category,
	ep.subcat AS subcategory,
    ep.maintenance,
    cp.prd_cost AS cost,
    cp.prd_line AS product_line,
    cp.prd_start_dt AS start_date
    
FROM silver.crm_prd_info cp
LEFT JOIN silver.erp_px_cat_g1v2 ep
	ON cp.cat_id = ep.id
WHERE cp.prd_end_dt IS NULL -- Filter out all historical data and keep latest records  
;    
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

CREATE VIEW gold.fact_sales2 AS
SELECT 
	sls_ord_num AS order_number,
    pr.product_key,
    cu.customer_key,
    sls_order_dt AS order_date,
    sls_ship_dt AS ship_date,
    sls_due_dt AS due_date,
    sls_sales AS sales,
    sls_quantity AS quantity,
    sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products2 pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers2 cu
ON sd.sls_cust_id = cu.customer_id
;

