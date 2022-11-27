-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT  
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(totals.visits) as visits,
    SUM(totals.pageviews) as pageviews,
    SUM(totals.transactions) as transactions,
    SUM(totals.totalTransactionRevenue)/1000000 as revenue 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
    trafficSource.source as source,
    SUM(totals.visits) as total_visits,
    SUM(totals.Bounces) as total_no_of_bounces,
    (SUM(totals.Bounces)/SUM(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC


-- Query 3: Revenue by traffic source by week, by month in June 2017
WITH month AS 
(
  SELECT 
      "Month" AS time_type,
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS time,
      trafficSource.source AS source,
      SUM(totals.totalTransactionRevenue)/1000000 as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
  GROUP BY source, time
),
week AS
(
  SELECT 
      "Week" AS time_type,
      FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time,
      trafficSource.source AS source,
      SUM(totals.totalTransactionRevenue)/1000000 as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
  GROUP BY source, time
)

SELECT *
FROM month
UNION ALL
SELECT *
FROM week
ORDER BY revenue DESC;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH purchaser AS
(
    SELECT
        month,
        (SUM(total_pagesviews_per_user)/COUNT(fullVisitorId)) AS avg_pageviews_purchase
    FROM
    (
        SELECT
            FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
            fullVisitorId,
            SUM(totals.pageviews) AS total_pagesviews_per_user
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _table_suffix BETWEEN '20170601' AND '20170731'
            AND totals.transactions >= 1
        GROUP BY fullVisitorId, month
    )
    GROUP BY month
),
nonpurchaser AS
(
    SELECT
        month,
        (SUM(total_pagesviews_per_user)/COUNT(fullVisitorId)) AS avg_pageviews_non_purchase
    FROM
    (
        SELECT
            FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
            fullVisitorId,
            SUM(totals.pageviews) AS total_pagesviews_per_user
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _table_suffix BETWEEN '20170601' AND '20170731'
            AND totals.transactions IS NULL
        GROUP BY fullVisitorId, month
    )
    GROUP BY month
)

SELECT 
    purchaser.*,
    avg_pageviews_non_purchase
FROM purchaser
INNER JOIN nonpurchaser
    USING (month)
ORDER BY month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) as month,
    SUM(totals.transactions)/COUNT(distinct fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
GROUP BY month


-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) as month,
    ((SUM(totals.totalTransactionRevenue)/SUM(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE  totals.transactions IS NOT NULL
GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
SELECT 
    v2ProductName AS other_purchased_products,
    SUM(productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(product) AS product
WHERE fullVisitorId IN 
(
  SELECT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) AS hits,
  UNNEST(product) AS product
  WHERE v2ProductName = "YouTube Men's Vintage Henley"
      AND productRevenue IS NOT NULL
  GROUP BY fullVisitorId
)
    AND v2ProductName != "YouTube Men's Vintage Henley"
    AND productRevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC;


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH productview AS
(
      SELECT  
          FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
          COUNT(v2ProductName) AS num_product_view
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hit,
      UNNEST(product) AS product
      WHERE _table_suffix BETWEEN '20170101' AND '20170331'
         AND ecommerceaction.action_type = '2'
      GROUP BY month
),
addtocart AS
(
      SELECT  
          FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
          COUNT(v2ProductName) AS num_addtocart
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hit,
      UNNEST(product) AS product
      WHERE _table_suffix BETWEEN '20170101' AND '20170331'
         AND ecommerceaction.action_type = '3'
      GROUP BY month
),
purchase AS
(
      SELECT  
          FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
          COUNT(v2ProductName) AS num_purchase
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hit,
      UNNEST(product) AS product
      WHERE _table_suffix BETWEEN '20170101' AND '20170331'
         AND ecommerceaction.action_type = '6'
      GROUP BY month
)

SELECT 
    productview.*,
    num_addtocart,
    num_purchase,
    ROUND(num_addtocart/num_product_view*100,2) AS add_to_cart_rate,
    ROUND(num_purchase/num_product_view*100,2) AS purchase_rate
FROM productview 
INNER JOIN addtocart 
    USING(month) 
INNER JOIN purchase 
    USING(month)
ORDER BY month;

