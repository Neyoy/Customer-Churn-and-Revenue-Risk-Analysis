# Customer Churn & Revenue Risk Analysis

## Project Overview
An end-to-end data analysis project identifying at-risk customers 
and quantifying revenue exposure using RFM segmentation on a 
real-world e-commerce transactional dataset.

**Business Question:** Which customers are likely to churn 
and how much revenue is at risk?

---

## Tools
- MySQL 
- Power BI Desktop

## Dataset
UCI Online Retail II (2010–2011)
541,910 raw transactions | 4,338 customers after cleaning

---

## Key Findings

- Churn Rate: 33.4% (1,449 customers)
- Total Revenue at Risk: £1,819,242
- Champions (22.8% of customers) generate 57.75% of total revenue
- At Risk segment: highest priority retention target with £149,181 
  recoverable at 20% re-engagement rate

---

## Contents

| File | Description |
|------|-------------|
| `churn_pipeline.sql` | Complete MySQL pipeline including data cleaning, 
customer metrics, RFM scoring, segmentation and validation queries |

---

## Pipeline Structure

| Table | Grain | Purpose |
|-------|-------|---------|
| retail_transactions | Transaction | Clean analytics-ready transaction data |
| customer_metrics | Customer | Lifetime RFM input aggregations per customer |
| rfm_segments | Customer | Segments, risk levels and churn status |
| monthly_revenue | Month | Monthly trend data for dashboard |

---

## Dashboard
The interactive Power BI dashboard is available here:
[View Dashboard](https://app.powerbi.com/view?r=eyJrIjoiZDExODkzNGItYWJmOC00OWQ2LTk0NmUtMjRkMWRmODM1MjhhIiwidCI6Ijc2MTk0OTUzLTA1ZTMtNDZlNi1hMmI5LTQ3NmFkOGE5NGQ2ZSJ9)

## Portfolio
Full project write-up including methodology, findings and 
business recommendations is available here:
[View Portfolio](https://raheemwaliyi79-my-site-1.editor.wix.com/html/editor/web/renderer/edit/1c25bc10-352b-4b07-9f31-ab7368371092?metaSiteId=11ac0497-cbd6-4a16-8a8d-b77fe798c3fd)
