/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Action Performed:
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/

/*---------------- Loading silver.crm_cust_info ----------------------*/

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
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
     END as cst_marital_status,   		
    CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END as cst_gndr,    
    cst_create_date
FROM    
(SELECT *,
ROW_NUMBER()
	OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
 FROM crm_cust_info) t 
WHERE flag_last = 1 -- Most recent record per customer
;

/* ------------------ Loading silver.crm_prd_info-----------------------*/

INSERT INTO silver.crm_prd_info (
	prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)         
SELECT 
	prd_id,
	SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    prd_nm,
    prd_cost,
    CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        WHEN 'S' THEN 'Other Sales'
        ELSE  'n/a'
    END AS prd_line,    
    prd_start_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY)  AS prd_end_dt
FROM bronze.crm_prd_info;

/* ------------------ Loading silver.crm_sales_details-----------------------*/

INSERT INTO silver.crm_sales_details(
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
    CONVERT(sls_order_dt, DATE) as sls_order_dt,
    CONVERT(sls_ship_dt, DATE) as sls_ship_dt,
    CAST(sls_due_dt AS DATE) as sls_due_dt,
    CASE    
		WHEN sls_sales IS NULL or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)     
			THEN sls_quantity * ABS(sls_price)         
		ELSE sls_sales     
	END as sls_sales, 
    sls_quantity,
	CASE         
		WHEN sls_price IS NULL or sls_price <= 0     
			THEN sls_sales / NULLIF(sls_quantity, 0)        
            ELSE sls_price     
	END as sls_price     
FROM bronze.crm_sales_details 

/* ------------------ Loading silver.erp_cust_az12-----------------------*/

INSERT INTO silver.erp_cust_az12(
	cid,
    bdate,
    gen
)    
SELECT 
    CASE 
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LENGTH(CID))
        ELSE cid
    END AS CID,    
    CASE
		WHEN bdate  > current_date() THEN NULL
        ELSE bdate
    END AS bdate,    
    CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen    
FROM bronze.erp_cust_az12;

/* ------------------ Loading silver.erp_loc_a101-----------------------*/

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


/* ------------------ Loading silver.erp_px_cat_g1v2-----------------------*/

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

