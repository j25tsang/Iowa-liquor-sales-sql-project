--https://www.weareiowa.com/article/money/economy/black-velvet-still-favorite-among-iowans-as-state-reports-record-high-liquor-sales-iowa-alcoholic-beverages-division-abd/524-1bc372f8-5811-4993-8786-df5144ee0976
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
GROUP BY COUNTY)

SELECT *,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg) AS median, MIN(avg),MAX(avg)
from CTE
group by county, cte.avg






WITH cte AS (
  SELECT COUNTY, COUNT(*) AS store_count
  FROM STORE
  GROUP BY COUNTY
), cte2 AS (
  SELECT COUNTY, store_count,
    (COUNT(*) OVER (PARTITION BY COUNTY) - AVG(store_count) OVER (PARTITION BY NULL)) AS store_count_variance
  FROM cte
)
SELECT COUNTY, store_count, store_count_variance,
  AVG(store_count) OVER (PARTITION BY COUNTY) AS avg_store_count,
  (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY store_count)
   FROM cte
   WHERE COUNTY = cte2.COUNTY) AS median_store_count,
  MIN(store_count) OVER (PARTITION BY COUNTY) AS min_store_count,
  MAX(store_count) OVER (PARTITION BY COUNTY) AS max_store_count
FROM cte2
ORDER BY COUNTY;




WITH county_store_counts AS (
  SELECT county, COUNT(*) AS store_count
  FROM STORE
  GROUP BY county
)
SELECT county, store_count, avg_store_count, CAST(store_count - avg_store_count AS decimal) AS store_count_variance
FROM county_store_counts
ORDER BY store_count DESC

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

--

SELECT * from store

SELECT * FROM STORE
WHERE county = 'IOWA'
ORDER BY city

SELECT * FROM store as s
LEFT JOIN city as c
ON s.city = c.city AND s.county = c.county
WHERE c.county is null
ORDER BY s.city,s.county

SELECT * FROM store WHERE city ilike '%ADAIR%'
order by city

SELECT * FROM STORE
WHERE city = 'FORT DODGE'

SELECT * FROM STORE
WHERE county = 'WEBSTER'

SELECT * from city

SELECT * FROM store as s
LEFT JOIN city as c
ON s.city = c.city
WHERE c.county is null
ORDER BY s.city,s.county


SELECT * FROM store as s
LEFT JOIN city as c
ON s.city = c.city AND s.county = c.county
WHERE s.store_id = '4417'
ORDER BY s.city,s.county

SELECT *
  FROM store
 WHERE store_id IN ('3591','4417','4753','5397','3838','5879','5424','6174','6292','5106','3451','9049','6195','5916','5852','5298','5241','6062','3592','6155','5237','4018','4651','9002','4847','9902','4944','5116','3925','3735','5012','4410','4320','5700','5725','5194','3950','9901','6040','10129','4980','5933','4067','5998')
ORDER BY CITY 

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

SELECT * from cte



SELECT COUNT(*) FROM STORE
WHERE status = 'A'

SELECT * from store
where store ilike '%distill%'