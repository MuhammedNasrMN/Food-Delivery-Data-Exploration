-- We need a table to show the Fail Rate per Talabat week.

SELECT 
(1-(SUM(orders.is_successful))/COUNT(orders.is_successful))*100 AS failure_rate,
STR_TO_DATE(dis_dates.talabat_week,'%d-%b-%Y') AS talabat_week
FROM (SELECT DISTINCT talabat_week, iso_date FROM dates) AS dis_dates
LEFT JOIN orders ON STR_TO_DATE(orders.order_time,'%m/%d/%Y') = STR_TO_DATE(dis_dates.iso_date,'%m/%d/%Y')
GROUP BY talabat_week
ORDER BY STR_TO_DATE(dis_dates.talabat_week,'%d-%b-%Y');

-- How many customers churned in December (i.e. ordered in November and did not in December)? 
-- 2487 customer churned in December
    
    WITH customer_activity AS (
    SELECT
        str_to_date(order_time, '%m/%d/%Y') AS month,
        analytical_customer_id
    FROM
        orders
)

SELECT
    DATE_FORMAT(c1.month,'%Y-%m'),
    COUNT(DISTINCT CASE WHEN c2.month IS NULL THEN c1.analytical_customer_id END) AS churned_customers,
    COUNT(DISTINCT CASE WHEN c2.month IS NOT NULL THEN c1.analytical_customer_id END) AS retained_customers
FROM
    customer_activity c1
LEFT JOIN
    customer_activity c2 ON c1.analytical_customer_id = c2.analytical_customer_id AND DATE_FORMAT(c1.month,'%Y-%m') =  DATE_FORMAT(DATE_ADD(c2.month, INTERVAL 1 MONTH), '%Y-%m')
-- WHERE c1.month >= '2021/10'
GROUP BY
    DATE_FORMAT(c1.month,'%Y-%m')
ORDER BY
    c1.month;
    
    
-- Segment those churned customers into 4 groups: “Frequent & Consistent”, “Frequent”, 
-- “Consistent”, “Neither”. Frequent customers are the ones that placed at least 12 orders 
-- in total from Sep to Nov. Consistent customers are the ones who did not miss 
-- a month without placing at least 1 order 


   WITH customer_activity AS (
    SELECT
        str_to_date(order_time, '%m/%d/%Y') AS months,
        analytical_customer_id
    FROM
        orders
)
SELECT
    analytical_customer_id,
    COUNT(DISTINCT DATE_FORMAT(months,'%Y-%m')) AS total_months,
    COUNT(DISTINCT CASE WHEN DATE_FORMAT(months,'%Y-%m') = '2021-12' THEN 1 END) AS churned,
    CASE
		WHEN COUNT(DISTINCT DATE_FORMAT(months,'%Y-%m')) >= 3 AND COUNT(analytical_customer_id) >= 12 AND COUNT(DISTINCT CASE WHEN DATE_FORMAT(months,'%Y-%m') = '2021-12' THEN 1 END) = 0 THEN 'Frequent & Consistent'
        -- WHEN COUNT(DISTINCT DATE_FORMAT(months,'%Y-%m')) >= 12 THEN 'Frequent'
        WHEN COUNT(analytical_customer_id) >= 12 AND COUNT(DISTINCT CASE WHEN DATE_FORMAT(months,'%Y-%m') = '2021-12' THEN 1 END) = 0 THEN 'Frequent'
		WHEN COUNT(DISTINCT DATE_FORMAT(months,'%Y-%m')) = 3 AND COUNT(DISTINCT CASE WHEN DATE_FORMAT(months,'%Y-%m') = '2021-12' THEN 1 END) = 0 THEN 'Consistent'
		WHEN COUNT(DISTINCT DATE_FORMAT(months,'%Y-%m')) < 3 AND COUNT(analytical_customer_id) < 12 AND COUNT(DISTINCT CASE WHEN DATE_FORMAT(months,'%Y-%m') = '2021-12' THEN 1 END) = 0 THEN 'Neither'
        ELSE 'Not Churned'
    END AS customer_segment
FROM
    customer_activity
WHERE DATE_FORMAT(months,'%Y-%m')<= '2021-12'
GROUP BY
    analytical_customer_id
ORDER BY customer_segment;

 /* We want to track if our customers pay more with each new order.
 For this, we need to know: In how many orders did the customer pay more than 
 their previous order? Ignore each customer's first order since we cannot know
 if it was more than the previous one. 

 Number of times order value increased is 41085
 Number of times order value decreased is 41860 */

SELECT order_value_condition, count(order_value_condition) order_value_count FROM(SELECT
	order_id, order_time, gmv_amount_lc,
    vendor_id, analytical_customer_id, is_successful,
    LAG(gmv_amount_lc) OVER (PARTITION BY analytical_customer_id ORDER BY order_time) AS previous_order_value
	, gmv_amount_lc-LAG(gmv_amount_lc) OVER (PARTITION BY analytical_customer_id ORDER BY order_time) AS difference
    ,     CASE
			WHEN gmv_amount_lc > LAG(gmv_amount_lc) OVER (PARTITION BY analytical_customer_id ORDER BY order_time) THEN 'Increased'
			WHEN gmv_amount_lc < LAG(gmv_amount_lc) OVER (PARTITION BY analytical_customer_id ORDER BY order_time) THEN 'Decreased'
			END AS order_value_condition
FROM 
    orders) o
WHERE order_value_condition IS NOT NULL AND is_successful=true
GROUP BY order_value_condition
ORDER BY 
    analytical_customer_id, order_time
;


-- calculate the MTD (Month-To-Date) active customers for every day. 

SELECT
  order_date,
  Orders,
  SUM(Orders) OVER (PARTITION BY EXTRACT(YEAR_MONTH FROM order_date) ORDER BY order_date) AS MTD
FROM
  (SELECT count(is_successful) AS orders, 
		str_to_date(order_time,'%m/%d/%Y') as order_date
        FROM orders
        WHERE is_successful = true
        GROUP BY order_date) a
ORDER BY
  order_date;
 
 /* Highest MTD by day 16 happened in September
  MTD (16/9) = 26495 */
  
WITH MonthlyTotals AS (SELECT
  order_date,
  Orders,
  SUM(Orders) OVER (PARTITION BY EXTRACT(YEAR_MONTH FROM order_date) ORDER BY order_date) AS MTD
FROM
  (SELECT count(is_successful) AS orders, 
		str_to_date(order_time,'%m/%d/%Y') as order_date
        FROM orders
        WHERE is_successful = true
        GROUP BY order_date) a
ORDER BY
  order_date
)
SELECT
  order_date,
  MAX(MTD) AS max_MTD
FROM
  MonthlyTotals
WHERE
  date_format(order_date, '%d') = 16
GROUP BY
  order_date
ORDER BY MTD DESC;
  
  
  
 /* same MTD calculation but for % of retained customers.
 The query for this question isn't correct since I couldn't find a way to only
sum distinct customers by each passing day in the month. */
  
SELECT order_date, 
  SUM( retained_customers) OVER (PARTITION BY EXTRACT(YEAR_MONTH FROM order_date) ORDER BY order_date) AS retained_customers_MTD
FROM( SELECT
    str_to_date(c1.order_time, '%m/%d/%Y') as order_date,
    COUNT(DISTINCT c1.analytical_customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN str_to_date(c2.order_time, '%m/%d/%Y') IS NOT NULL THEN c1.analytical_customer_id END) AS retained_customers
FROM
    orders c1
LEFT JOIN
    orders c2 ON c1.analytical_customer_id = c2.analytical_customer_id AND DATE_FORMAT(str_to_date(c1.order_time, '%m/%d/%Y'),'%Y-%m') =  DATE_FORMAT(DATE_ADD(str_to_date(c2.order_time, '%m/%d/%Y'), INTERVAL 1 MONTH), '%Y-%m')
-- WHERE c1.month >= '2021/10'
GROUP BY
    str_to_date(c1.order_time, '%m/%d/%Y')) a
