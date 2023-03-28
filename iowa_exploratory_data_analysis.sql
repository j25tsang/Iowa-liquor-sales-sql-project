
----------General stats related to iowa's population and store count----------------
--Questions related to population density in search for a prime location

--top counties by population 
SELECT county, SUM(population) FROM CITY
WHERE year = 2021
GROUP BY county
ORDER BY SUM(population) DESC

--top cities by population
SELECT county, city, SUM(population)  FROM CITY
WHERE year = 2021
GROUP BY county, city
ORDER BY SUM(population) DESC

-- top counties by number of stores . Excluding inactive stores
SELECT DISTINCT county, count(*) OVER (PARTITION BY county) AS store_count 
FROM STORE
WHERE status = 'A'
ORDER BY store_count DESC
LIMIT 50

--mean, medium, mode of Iowa total liquor stores
SELECT AVG(counter), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY counter) AS median, MIN(counter),MAX(COUNTER), SUM(COUNTER)
FROM (SELECT COUNT(*) AS counter FROM STORE WHERE status = 'A' GROUP BY COUNTY ORDER BY COUNT(*)) AS subquery;

-- top 20 cities by number of stores 
SELECT DISTINCT county, city, count(*) OVER (PARTITION BY city) AS store_count 
FROM STORE
WHERE status = 'A'
ORDER BY store_count DESC
LIMIT 20

--percentage of stores per county in Iowa
WITH CTE AS(
SELECT DISTINCT county, count(*) OVER (PARTITION BY county) AS store_count 
FROM STORE
	WHERE status = 'A'
ORDER BY store_count DESC)

SELECT 
CASE WHEN ROUND(store_count / sum(store_count) OVER (),2) <= 0.01 THEN 'OTHER' ELSE county END AS county,
store_count, ROUND(store_count / sum(store_count) OVER (),2) AS percentage_store_count
from cte
ORDER BY store_count DESC

--creating a view from CTE as table_a -- 
select county, round(avg(store_count),0) as store_count, sum(percentage_store_count) as percentage_store_count from table_a
GROUP BY county
ORDER BY store_count DESC


-- percentage concentration of stores per city per county. filtering for only active stores, we have about 50 less stores
WITH CTE AS(
SELECT DISTINCT county, city,
count(*) OVER (PARTITION BY county) AS county_store_count, count(*) OVER (PARTITION BY city) AS city_store_count,  
ROUND((CAST(count(*) OVER (PARTITION BY city) AS decimal) / cast(count(*) OVER (PARTITION BY county) AS decimal)),2) AS percentage_store_count
FROM STORE
WHERE STATUS = 'A'
ORDER BY city_store_count DESC
)

SELECT * from cte
WHERE county_store_count > 16 

-- 1914 stores in iowa made active orders for the past 3 months 
WITH CTE AS(
SELECT s.store_id, s.store, s.status, s.address, s.zip_code, s.county, o.date FROM STORE as s
LEFT JOIN orders as o
ON s.store_id = o.store_id
WHERE o.date >= '2022-12-15' AND o.date <= '2023-03-15' AND s.status = 'A'
)
SELECT COUNT(DISTINCT store_id)
FROM cte

-- active store count vs. inactive store count - year over year by county - including YoY growth and store renttention rate. 
WITH CTE AS (
SELECT county, left(cast(date as varchar),4) AS date, cast(count(distinct o.store_id) as decimal) AS active_stores
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE s.status = 'A' AND county IN ('POLK','LINN','SCOTT','JOHNSON','BLACK HAWK')
GROUP BY county, left(cast(date as varchar),4)
ORDER BY county, date)

SELECT a.county,a.date,a.active_stores,b.active_stores AS inactive_stores,
round((a.active_stores - lag(a.active_stores) OVER (PARTITION BY a.county ORDER BY a.date))/lag(a.active_stores) OVER (PARTITION BY a.county ORDER BY a.date),2) AS yoy_growth,
round((a.active_stores/(a.active_stores + b.active_stores)),2) AS store_retention_rate
FROM CTE as a
INNER JOIN (SELECT county, left(cast(date as varchar),4) AS date, cast(count(distinct o.store_id) as decimal) AS active_stores
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE s.status = 'I' AND county IN ('POLK','LINN','SCOTT','JOHNSON','BLACK HAWK')
GROUP BY county, left(cast(date as varchar),4)
ORDER BY county, date) AS b
ON a.county = b.county AND a.date = b.date


--

----

WITH CTE AS(
SELECT s.store_id, s.store, s.status, s.address, s.zip_code, s.county, o.date FROM STORE as s
LEFT JOIN orders as o
ON s.store_id = o.store_id
WHERE o.date >= '2023-01-01' AND o.date <= '2023-03-15' AND s.status = 'A'
)

SELECT distinct Store_id, store, status, address, zip_code, county, left(cast(date as varchar),7) as date from CTE
ORDER by date desc


----------General stats related to order quantity and cost ----------------
-- Questions gauging liquor demand in search for a prime location

--Top county by liquor demand cost & quantity last 12 months
SELECT county, SUM(order_cost) AS total_cost
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE o.date >= '2022-03-15' AND o.date <= '2023-03-15'
GROUP BY county
ORDER BY total_cost DESC

SELECT county, SUM(order_qty) AS total_qty
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE o.date >= '2022-03-15' AND o.date <= '2023-03-15'
GROUP BY county
ORDER BY total_qty DESC

-- year over year demand by top 5 most populated counties since 2016
SELECT county, left(cast(date as varchar),4) AS date, SUM(order_cost) AS total_cost
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE county IN ('POLK','LINN','SCOTT','JOHNSON','BLACK HAWK') AND left(cast(date as varchar),4) > '2015'
GROUP BY county, left(cast(date as varchar),4)
ORDER BY county, date desc

SELECT county, left(cast(date as varchar),4) AS date, SUM(order_qty) AS total_qty
FROM orders as o
INNER JOIN store as s
ON o.store_id = s.store_id
WHERE county IN ('POLK','LINN','SCOTT','JOHNSON','BLACK HAWK')
GROUP BY county, left(cast(date as varchar),4)
ORDER BY county, date desc




select * from orders
limit 100



--Top counties by Class "E" liquor sales
--Top Iowa brands by gallons sold
--Top liquor brands by gallons sold




