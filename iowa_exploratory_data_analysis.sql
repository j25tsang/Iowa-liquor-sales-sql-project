-- General insights below
--Total gallons sold per county
--Top counties by Class "E" liquor sales
--Top Iowa brands by gallons sold
--Top liquor brands by gallons sold

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

--average store count
-- Calculate the average store count for each county
SELECT DISTINCT county, count(*) OVER (PARTITION BY county) AS store_count
FROM (SELECT COUNT(*) AS counter FROM STORE GROUP BY COUNTY ORDER BY COUNT(*)) AS subquery


WITH CTE AS (
SELECT COUNTY, AVG(counter) 
FROM (SELECT COUNTY, COUNT(store_id) AS counter FROM STORE GROUP BY COUNTY ORDER BY COUNT(*)) AS subquery
GROUP BY COUNTY
)

SELECT *,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg) AS median, MIN(avg),MAX(avg)
from CTE
group by county, cte.avg


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
WHERE STATUS = 'I'
ORDER BY city_store_count DESC
)

SELECT * from cte
WHERE county_store_count > 16 


-- 2003 stores in iowa made active orders for the past 6 months 
WITH CTE AS(
SELECT s.store_id, s.store, s.status, s.address, s.zip_code, s.county, o.date FROM STORE as s
LEFT JOIN orders as o
ON s.store_id = o.store_id
WHERE o.date >= '2023-01-01' AND o.date <= '2023-03-15'
)
SELECT distinct Store_id, store, status, address, zip_code, county, left(cast(date as varchar),7) as date from CTE
WHERE status = 'I'
ORDER by date desc

