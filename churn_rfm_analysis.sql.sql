-- ============================================================
-- PROJECT   : Customer Churn & Revenue Risk Analysis
-- DATASET   : Online Retail II (2010–2011)
-- INDUSTRY  : E-commerce
-- OBJECTIVE : Identify churned customers, apply RFM segmentation,
--             and quantify revenue at risk to support
--             targeted retention strategy
-- ============================================================

CREATE DATABASE online_retail;
USE online_retail;

-- CREATE A TABLE

CREATE TABLE online_retail_2010_2011 (
invoice_no	VARCHAR(50),
stock_code VARCHAR(50),
quantity INT,
invoice_date	DATE,
price	DECIMAL(10,2),
customer_id	VARCHAR(50),
country VARCHAR(100)
);


-- LOAD DATA INTO THE TABLE CREATED
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/online_retail_II_2010_2011.csv'
INTO TABLE online_retail_2010_2011
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(@Invoice, @StockCode, @Quantity, @InvoiceDate, @Price, @Customer_ID, @Country)
SET 
  invoice_no = @Invoice,
  stock_code = @StockCode,
  quantity = @Quantity,
  invoice_date = STR_TO_DATE(@InvoiceDate,'%Y-%m-%d'),
  price = NULLIF(@Price,''),
  customer_id = @Customer_ID,
  country = @Country;

SELECT *
FROM online_retail_2010_2011
LIMIT 10;

SELECT COUNT(*)
FROM online_retail_2010_2011;

-- COPY THE DATA TO ANOTHER TABLE TO SECURE THE RAW DATASET.
CREATE TABLE retail_stage
LIKE online_retail_2010_2011;

INSERT INTO retail_stage
SELECT *
FROM online_retail_2010_2011;

SELECT *
FROM retail_stage
LIMIT 10;

SELECT MIN(invoice_date) start_date, MAX(invoice_date) end_date
FROM retail_stage;


/* 
============================================================
DATA CLEANING
============================================================
 Remove Invalid or Unusable Records
 */
 
-- Remove rows with missing customer IDs as churn analysis requires customer tracking.

SELECT *
FROM retail_stage
WHERE customer_id = '';

-- 135080 rows with missing IDs were removed
DELETE 
FROM retail_stage
WHERE  customer_id = '';


/* 
Remove cancelled transactions
Invoices starting with C represent returns.
-- 8905 rows were removed
*/

DELETE
FROM retail_stage
WHERE invoice_no LIKE 'C%';

/* 
Remove negative quantities
-- Zero rows affected
*/

DELETE
FROM retail_stage
WHERE quantity <= 0;

/*
Remove invalid prices
-- 44 rows were removed
*/

DELETE
FROM retail_stage
WHERE price <= 0;

-- Check for Exact Duplicate Rows
SELECT 
invoice_no,
stock_code,
quantity,
invoice_date,
price,
customer_id,
country,
COUNT(*) AS duplicate_count
FROM retail_stage
GROUP BY
invoice_no,
stock_code,
quantity,
invoice_date,
price,
customer_id,
country
HAVING COUNT(*) > 1;


-- Remove Exact Duplicates
CREATE TABLE retail_stage_clean AS
SELECT DISTINCT *
FROM retail_stage;


-- ============================================================
-- PIPELINE OVERVIEW
-- ┌─────────────────────────┐
-- │  retail_stage_clean     │  ← Cleaned raw data (already built)
-- └────────────┬────────────┘
--              │
-- ┌────────────▼────────────┐
-- │  retail_transactions    │  ← Structured transaction table
-- └────────────┬────────────┘
--              │
-- ┌────────────▼────────────┐
-- │  customer_metrics       │  ← Customer-level aggregations
-- └────────────┬────────────┘
--              │
-- ┌────────────▼────────────┐
-- │  rfm_scored             │  ← RFM scores per customer
-- └────────────┬────────────┘
--              │
-- ┌────────────▼────────────┐
-- │  rfm_segments           │  ← Segments + risk + churn labels
-- └─────────────────────────┘

-- ============================================================
--  retail_transactions
-- PURPOSE : Structured, analytics-ready transaction table.
--           Adds a pre computed revenue column to avoid
--           repeating quantity * unit_price in every query.
-- SOURCE  : retail_stage_clean
-- ============================================================

CREATE TABLE retail_transactions AS
SELECT
invoice_no,
stock_code,
quantity,
invoice_date,
price As unit_price,
quantity * price AS revenue,
customer_id,
country
FROM retail_stage_clean;

-- Verification
SELECT * 
FROM retail_transactions
LIMIT 10;

SELECT
COUNT(*) AS						total_rows,
COUNT(DISTINCT customer_id) AS 	total_customers,
COUNT(DISTINCT invoice_no) AS  	unique_invoice,
ROUND(SUM(revenue),2) AS		total_revenue,
MIN(invoice_date) AS 			date_from,
MAX(invoice_date) AS 			date_to
FROM retail_transactions;


-- ============================================================
--  customer_metrics
-- PURPOSE : Aggregates transaction data to the customer level.
--           Computes the three core RFM inputs — Recency,
--           Frequency, and Monetary — alongside purchase
--           history metadata for each customer.
-- SOURCE  : retail_transactions
-- GRAIN   : One row per customer
-- ============================================================

CREATE TABLE customer_metrics AS
SELECT 
customer_id,
MIN(invoice_date) AS 										first_purchase,
MAX(invoice_date) AS 										last_purchase,
DATEDIFF(
	(SELECT MAX(invoice_date) FROM retail_transactions),
    MAX(invoice_date))
AS 															recency,
COUNT(DISTINCT invoice_no) AS 								frequency,
SUM(revenue) AS 											monetary,
SUM(quantity) AS 											total_quantity
FROM retail_transactions
GROUP BY customer_id;

-- Verification
SELECT *
FROM customer_metrics
LIMIT 10;

SELECT
COUNT(customer_id)  	AS total_customers,
ROUND(AVG(recency))    		AS avg_recency_days,
ROUND(AVG(frequency))	AS avg_orders,
ROUND(AVG(monetary)) 	AS avg_spend
FROM customer_metrics;

-- ============================================================
--  rfm_scored
-- PURPOSE : Assigns each customer a score of 1–4 on each
--           RFM dimension using NTILE quartile ranking.
--           Score 4 = best (most recent, most frequent,
--           highest spender). Score 1 = worst.
-- SOURCE  : customer_metrics
-- GRAIN   : One row per customer
-- ============================================================

CREATE TABLE rfm_scored AS
SELECT
	customer_id,
    first_purchase,
    last_purchase,
    recency,
    frequency,
    monetary,
    NTILE(4) OVER (ORDER BY recency DESC) 		AS r_score,
    NTILE(4) OVER (ORDER BY frequency ASC)			AS f_score,
    NTILE(4) OVER (ORDER BY monetary ASC) 	AS m_score
FROM customer_metrics;

-- verification
SELECT *
FROM rfm_scored
LIMIT 10;

-- Check score distribution is balanced across quartiles
SELECT
r_score,
COUNT(customer_id) AS customers
FROM rfm_scored
GROUP BY r_score
ORDER BY r_score DESC;

-- ============================================================
--  rfm_segments
-- PURPOSE : Applies business logic to RFM scores to classify
--           each customer into a named segment, assign a
--           revenue risk level, and flag churn status.
-- SOURCE  : rfm_scored
-- GRAIN   : One row per customer
-- ============================================================

CREATE TABLE rfm_segments AS
SELECT 
customer_id,
    first_purchase,
    last_purchase,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
 
    -- RFM Segment
    -- Logic: combined r_score and f_score determine
    -- how recently and how often a customer bought,
    CASE
        WHEN r_score = 4 AND f_score >= 3 THEN 'Champion'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'Potential Loyalist'
        WHEN r_score = 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score = 2 AND f_score <= 2 THEN 'Hibernating'
        WHEN r_score = 1 THEN 'Lost'
        ELSE 'Other'
    END AS segment,
    
     -- Revenue Risk Level
    -- Derived from segment to indicate urgency of intervention
    CASE
        WHEN r_score = 1                   THEN 'Critical'
        WHEN r_score = 2 AND f_score >= 3  THEN 'High'
        WHEN r_score = 2 AND f_score <= 2  THEN 'Medium'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'Low'
        ELSE 'Safe'
    END AS risk_level,
 
    -- Churn Status
    -- Definition: no purchase in 90+ days = Churned
    CASE
        WHEN recency >= 90 THEN 'Churned'
        ELSE 'Active'
    END AS churn_status
 
FROM rfm_scored;
 
-- Verification
SELECT * 
FROM rfm_segments LIMIT 10;
    
    
-- ============================================================
--  monthly_revenue
-- PURPOSE : Aggregates transaction data by month to support
--           trend analysis in the dashboard. Kept as a
--           separate table because it operates at a
--           different grain (monthly vs customer level).
-- SOURCE  : retail_transactions
-- GRAIN   : One row per month
-- ============================================================

CREATE TABLE monthly_revenue AS
SELECT
	DATE_FORMAT(invoice_date, '%Y-%m')		AS `month`,
	COUNT(DISTINCT customer_id)				AS active_customers,
	COUNT(DISTINCT invoice_no)				AS total_orders,
	ROUND(SUM(revenue),2)					AS revenue
FROM retail_transactions
GROUP BY `month`
ORDER BY `month`;

-- verification
SELECT 
`month`,
active_customers,
total_orders,
revenue
FROM monthly_revenue
ORDER BY revenue;

-- ============================================================
--  ANALYTICAL VALIDATION QUERIES
-- ============================================================
 
-- Churn summary
SELECT 
churn_status,
COUNT(*) total_customers,
ROUND(COUNT(*) *100 / SUM(COUNT(*)) OVER(),2) AS percentage
FROM rfm_segments
GROUP BY churn_status;

-- Segment distribution

SELECT
segment,
COUNT(*) 	AS total_customers,
ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(),2) 	AS percentage,
ROUND(AVG(recency))		AS avg_recency_days,
ROUND(AVG(frequency)) 	AS avg_orders,
ROUND(AVG(monetary))		AS avg_spend
FROM rfm_segments
GROUP BY segment
ORDER BY total_customers DESC;

--  Revenue per segment
SELECT
segment,
ROUND(SUM(monetary),2)	AS total_revenue,
ROUND(AVG(monetary),2)	AS avg_order_value
FROM rfm_segments
GROUP BY segment
ORDER BY total_revenue DESC;

-- Revenue at risk with recovery scenarios


SELECT
segment,
risk_level,
COUNT(customer_id)	AS customers_at_risk,
ROUND(SUM(monetary),2) 		AS revenue_at_risk,
ROUND(SUM(monetary) * 0.10,2) 		AS recovery_10_pct,
ROUND(SUM(monetary) * 0.20,2) 		AS recovery_20_pct
FROM rfm_segments
WHERE segment IN ('At Risk', 'Lost', 'Hibernating')
GROUP BY segment, risk_level
ORDER BY revenue_at_risk DESC;

-- Champions vs everyone else
SELECT
CASE
WHEN segment = 'Champion' THEN 'Champions'
ELSE 'everyone else'
END 		AS customer_group,
ROUND(SUM(monetary),2)			AS total_revenue,
ROUND(SUM(monetary) * 100 / SUM(SUM(monetary)) OVER() ,2) revenue_share_pct
FROM rfm_segments
GROUP BY customer_group
ORDER BY total_revenue DESC;


--  Full risk summary
SELECT
    segment,
    risk_level,
    COUNT(DISTINCT customer_id)                      AS customers,
    ROUND(SUM(monetary), 2)                           AS total_revenue,
    ROUND(AVG(recency))                              AS avg_days_inactive
FROM rfm_segments 
GROUP BY segment, risk_level
ORDER BY total_revenue DESC;


-- ============================================================
-- FINAL DATABASE STRUCTURE
-- ============================================================
--
--  TABLE                             PURPOSE
--  ─────────────────────────────────────────────────────────
--  online_retail_2010_2011      Raw backup — untouched
--  retail_transactions          Clean analytics base
--  customer_metrics             RFM input aggregations
--  rfm_scored                   Quartile scores (1–4)
--  rfm_segments                 Segments + risk + churn
--  monthly_revenue              Trend analysis

-- ============================================================
-- KEY FINDINGS
-- ─────────────────────────────────────────────────────────
--  Total Customers    : 4,338
--  Total Revenue      : £8,886,670
--  Churn Rate         : 33.4%  (1,449 customers)
--  Revenue at Risk    : £1,819,586
--  Champions Share    : 57.75% of total revenue




