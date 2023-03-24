-- Create table from denormalized datasource
CREATE TABLE orders(
	order_id VARCHAR(50) PRIMARY KEY,
	date VARCHAR(20),
	store_id VARCHAR(20),
	store VARCHAR(100),
	address VARCHAR(100),
	city VARCHAR(50),
	zip_code VARCHAR(20),
	location VARCHAR(50),
	county_id VARCHAR(20),
	county VARCHAR(20),
	category_id VARCHAR(20),
	category VARCHAR(50),
	vendor_id VARCHAR(20),
	vendor VARCHAR(100),
	product_id VARCHAR(20),
	product VARCHAR,
	pack_size INTEGER,
	volume_ml VARCHAR(10),
	state_bottle_cost VARCHAR(10),
	state_bottle_price VARCHAR(10),
	unit_sold VARCHAR,
	order_cost VARCHAR,
	order_liters VARCHAR,
	order_gallons VARCHAR
);

-- Import csv data to table
--\COPY orders FROM 'C:/Users/Public/Iowa_Liquor_Sales.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
COPY orders FROM 'C:\Users\Public\Iowa_Liquor_Sales.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';


-- Cast string with comma (e.g. 1,000) into integer 
SELECT regexp_replace(bottle_cost,'[^0-9]+', '', 'g')::decimal FROM temp
LIMIT 100

-- update table columns with correct data type
ALTER TABLE orders
ALTER bottle_ml type int using regexp_replace(bottle_ml, '[^0-9]+', '', 'g')::int
ALTER state_bottle_cost type decimal using regexp_replace(state_bottle_cost,',', '')::decimal
ALTER state_bottle_price type decimal using regexp_replace(state_bottle_price,',', '')::decimal
ALTER order_qty type int using regexp_replace(order_qty,',', '')::int
ALTER order_cost type decimal using regexp_replace(order_cost,',', '')::decimal
ALTER order_liters type decimal using regexp_replace(order_liters,',', '')::decimal
ALTER order_gallons type decimal using regexp_replace(order_gallons,',', '')::decimal

--Create products table 
--ERROR:  invalid input syntax for type numeric: "2,000.00" (solution - VARCHAR)
CREATE TABLE product(
	product_id VARCHAR(20) PRIMARY KEY,
	category VARCHAR(50),
	product VARCHAR,
	vendor_id VARCHAR(20),
	vendor VARCHAR(100),
	bottle_ml VARCHAR(50),
	pack_size INTEGER,
	innerpack_size INTEGER,
	age INTEGER,
	proof INTEGER,
	list_date VARCHAR(20),
	upc VARCHAR(50),
	scc VARCHAR(50),
	state_bottle_cost VARCHAR(20),
	state_case_cost VARCHAR(20),
	state_bottle_price VARCHAR(20),
	report_date VARCHAR(20)
	)
	
--\COPY product FROM 'C:/Users/Public/Iowa_Liquor_Products.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
COPY product FROM 'C:\Users\Public\Iowa_Liquor_Products.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- update product table columns with correct data type
ALTER TABLE product
ALTER bottle_ml type int using regexp_replace(volume_ml,',', '')::int
ALTER state_bottle_cost type decimal using regexp_replace(unit_cost,',', '')::decimal
ALTER state_case_cost type decimal using regexp_replace(case_cost,',', '')::decimal
ALTER state_bottle_price type decimal using regexp_replace(retail_price,',', '')::decimal
ALTER list_date type date using to_date(list_date,'MM/DD/YYYY')::date
ALTER report_date type date using to_date(report_date,'MM/DD/YYYY')::date

--create vendor table
CREATE TABLE vendor AS(
SELECT DISTINCT vendor_id, vendor FROM product
ORDER BY vendor_id);

--Recreate product table to fix duplicates in category column
CREATE TABLE products AS(
SELECT product_id, list_date, product, age, proof, vendor_id,
CASE WHEN category = 'AMERICAN CORDIALS & LIQUEURS' THEN 'AMERICAN CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED CORDIALS & LIQUEURS' THEN 'IMPORTED CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED DISTILLED SPIRITS SPECIALTY' THEN 'IMPORTED DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'NEUTRAL GRAIN SPIRITS FLAVORED' THEN 'NEUTRAL GRAIN SPIRITS'
	ELSE category
END AS category, pack_size, innerpack_size, bottle_ml, state_bottle_cost, state_case_cost, state_bottle_price
FROM product
ORDER BY product_id)

--Check if products table matches all products in orders table
SELECT list_date, products.product_id, products.product, orders.product_id, orders.product
FROM products
LEFT JOIN orders
ON products.product_id = orders.product_id
WHERE orders.product_id IS NULL OR orders.product IS NULL

--DELETE null rows from product table as specified by LEFT JOIN ON orders table. 
--DELETE null rows from left join to ensure product_ids from both tables match 
DELETE FROM products AS d
WHERE NOT EXISTS (
	SELECT * FROM orders AS r
    WHERE d.product_id = r.product_id);
	
--Check if vendor table matches all vendors in orders table
SELECT vendor.vendor_id, vendor.vendor, orders.vendor_id, orders.vendor
FROM vendor
LEFT JOIN orders
ON vendor.vendor_id = orders.vendor_id
WHERE orders.vendor_id IS NULL OR orders.vendor IS NULL

--DELETE null rows from left join to ensure vendor_ids from both tables match 
DELETE FROM vendor AS d
WHERE NOT EXISTS (
	SELECT * FROM orders AS r
    WHERE d.vendor_id = r.vendor_id);
	
--deleted original product table as it is no longer needed
DROP TABLE product

--CREATE new product table from ORDERS table
CREATE TABLE product AS (
SELECT DISTINCT product_id, to_date(date,'MM/DD/YYYY') AS date, product, vendor_id, category, pack_size, bottle_ml, state_bottle_cost, state_bottle_price
FROM orders
GROUP BY product_id, to_date(date,'MM/DD/YYYY'), product, vendor_id, category, pack_size, bottle_ml, state_bottle_cost, state_bottle_price
ORDER BY product_id, to_date(date,'MM/DD/YYYY')
	)

-- Check for null values in orders.vendor and match against the products table to find the missing value

SELECT orders.product_id,orders.product, orders.vendor_id, orders.vendor,products.product_id, products.product, products.vendor_id, coalesce(orders.vendor_id,products.vendor_id)
FROM orders
INNER JOIN products
ON orders.product_id = products.product_id
WHERE orders.vendor is null or orders.vendor_id IS NULL

--Update missing vendor_id values
UPDATE orders
SET vendor_id = products.vendor_id
FROM products
WHERE orders.product_id = products.product_id AND orders.vendor_id IS NULL

--
SELECT b.store_id, b.store, b.address, d.address, d.city
FROM orders AS b
LEFT JOIN orders AS d ON b.store_id = d.store_id


SELECT * from ORDERS
WHERE address is null

SELECT * FROM orders 
WHERE store_id = '4000'

-- update the null values in the Address-related columns of order table based on another row referencing the same store:
-- this was not executed in the end. Not needed as proposed a different solution below

UPDATE orders
SET 
  Address = (SELECT address 
             FROM orders AS A2 
             WHERE A2.Store_id = orders.Store_id
               AND A2.Address IS NOT NULL 
             LIMIT 1),
  City = (SELECT City 
          FROM orders AS A2 
          WHERE A2.Store_id = orders.Store_id
            AND A2.City IS NOT NULL 
          LIMIT 1),

WHERE 
  Address IS NULL OR 
  City IS NULL;
  
-- Query for all store related data 
SELECT DISTINCT store_id, store, address, city, zip_code, location, county
FROM orders
WHERE location IS NULL
ORDER BY STORE_ID

-- Create store table from order table for normalization purposes
CREATE table store AS (
SELECT store_id, MIN(store) AS store, MIN(address) AS address, MIN(city) AS city, MIN(zip_code) AS zip_code, MIN(location) AS location, MIN(county) AS county
FROM orders
GROUP BY store_id
ORDER BY store_id
	)
	
-- create stores table 
CREATE TABLE stores (
    store_id VARCHAR(20) PRIMARY KEY,
    store VARCHAR(100),
    status VARCHAR(10),
    address VARCHAR(100),
    city VARCHAR(50),
    zip_code VARCHAR(5),
    location VARCHAR,
    report_date VARCHAR
);

-- import stores.csv table
COPY stores FROM 'C:\Users\Public\Iowa_Liquor_Stores.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

--consolidate store information from two store tables. check if locations match
SELECT store.store_id,store.store,
CASE 
	WHEN stores.status IS NULL THEN 'I'
ELSE status
END AS status,store.address,store.city,store.zip_code,store.location,store.county, stores.location
from store
LEFT JOIN stores
ON store.store_id = stores.store_id
WHERE stores.location IS NOT NULL AND store.location IS NULL
ORDER BY store_id;

-- drop first iteration stores table replace with new cosolidated stores table
-- renamed store2 to stores

--check add latitude longitude columns
SELECT * from stores
SELECT *, substring(location, E'[^ ]+ ([^ ]+)\\)') AS latitude,
substring(location, E'\\(([^ ]+) [^ ]+\\)', '\\1') AS longitude
FROM stores;

--Alter location data with latitude longitude column
ALTER TABLE stores ADD COLUMN latitude numeric;
ALTER TABLE stores ADD COLUMN longitude numeric;

UPDATE stores SET
  latitude = substring(location, '[^ ]+ ([^ ]+)', '\\1')::numeric,
  longitude = substring(location, '([^-]+) ', '\\1')::numeric;
  
-- Alter store_id from varchar to integer data type including its values
ALTER TABLE orders
ALTER COLUMN store_id TYPE INTEGER
USING CAST(store_id AS INTEGER);

-- alters data type without altering column values
ALTER TABLE stores
ALTER COLUMN zip_code TYPE VARCHAR(5);

update store2
set location = null
WHERE location = '#REF!'

SELECT * from stores

select location, substring(location from '\(([^ ]+) ([^ ]+)\)') as longitude from stores
SELECT substring(trim(leading 'POINT (' from trim(trailing ')' from location)), strpos(trim(leading 'POINT (' from trim(trailing ')' from location)), ' ') + 1) as mid
FROM stores;

--check extract command for latitude and longitude from location column
SELECT location,
	cast((substring(trim(leading 'POINT (' from trim(trailing ')' from location)), strpos(trim(leading 'POINT (' from trim(trailing ')' from location)), ' ') + 1)) as decimal(10,7)) as latitude,
	cast((substring(location from '\(([^ ]+) ([^ ]+)\)')) as decimal(10,7)) as longitude
FROM stores;

-- add new columns latitude and longitude
ALTER TABLE stores ADD COLUMN latitude decimal(10,7);
ALTER TABLE stores ADD COLUMN longitude decimal(10,7);

-- Update new columns with extracted values from location column
UPDATE stores
SET latitude = cast((substring(trim(leading 'POINT (' from trim(trailing ')' from location)), strpos(trim(leading 'POINT (' from trim(trailing ')' from location)), ' ') + 1)) as decimal(10,7)),
    longitude = cast((substring(location from '\(([^ ]+) ([^ ]+)\)')) as decimal(10,7));

-- drop column 
ALTER TABLE stores
DROP COLUMN location;

-- update county names to ensure spelling consistency
update stores
set
county = CASE WHEN county = 'CERRO GORD' THEN 'CERRO GORDO'
			WHEN county = 'OBRIEN' THEN 'O''BRIEN'
			WHEN county = 'POTTAWATTA' THEN 'POTTAWATTAMIE'
			ELSE county
			END;
			
-- drop store table is it is no longer needed
drop table store

--
SELECT * from stores
--


SELECT Distinct product_id, max(state_bottle_price)  from product
GROUP BY product_id, state_bottle_cost
order by product_id


SELECT LEFT(to_char(date,'yyyy-mm-dd'),4)

--create new productss table
Create table productss AS(
SELECT DISTINCT ON(product_id) product_id, product, category, vendor_id, pack_size, bottle_ml, max(state_bottle_cost) OVER (partition by product_id) AS state_bottle_cost, max(state_bottle_price) OVER (partition by product_id) AS state_bottle_price 
from product
ORDER BY product_id, product)

--delete vendor_id column from product table. redundant 
ALTER TABLE product
DROP COLUMN vendor_id;

--create new product table from order table for normalization purposes
CREATE Table Product_distinct AS(
SELECT DISTINCT ON (o.product_id)
  o.product_id, o.product, o.vendor_id, c.category
FROM orders AS o
LEFT JOIN (
  SELECT product_id, MIN(category) AS category
  FROM orders
  WHERE category IS NOT NULL
  GROUP BY product_id
) AS c ON o.product_id = c.product_id
ORDER BY o.product_id)

--Delete product and category column from product table - redundant
ALTER TABLE product
DROP COLUMN product
DROP COLUMN category;



--update the category column in the product_distinct table based on matching product values in previous rows,
UPDATE product_distinct pd1
SET category = (
  SELECT pd2.category
  FROM (
    SELECT product, category
    FROM product_distinct
    WHERE category IS NOT NULL
  ) pd2
  WHERE pd2.product = pd1.product
  ORDER BY pd2.category DESC
  LIMIT 1
)
WHERE pd1.category IS NULL;

-- update categories to be consistent. SELECT clause to double-check desired output
SELECT
CASE WHEN category = 'AMERICAN CORDIALS & LIQUEURS' THEN 'AMERICAN CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED DISTILLED SPIRITS SPECIALTY' THEN 'IMPORTED DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'NEUTRAL GRAIN SPIRITS FLAVORED' THEN 'NEUTRAL GRAIN SPIRITS'
	WHEN category = 'AMERICAN DISTILLED SPIRITS SPECIALTY' THEN 'AMERICAN DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'AMERICAN VODKAS' THEN 'AMERICAN VODKA'
	WHEN category = 'COCKTAILS /RTD' THEN 'COCKTAILS / RTD'
	WHEN category = 'COCKTAILS/RTD' THEN 'COCKTAILS / RTD'
	WHEN category = 'FLAVORED GINS' THEN 'FLAVORED GIN'
	WHEN category = 'IMPORTED CORDIALS & LIQUEURS' THEN 'IMPORTED CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED DISTILLED SPIRITS SPECIALTY' THEN 'IMPORTED DISTILLED SPIRIT SPECIALTY'
	ELSE category
END as category2
FROM product_distinct
GROUP BY category2
ORDER BY category2

--create new category2 column in table and update with the desired values

ALTER TABLE product_distinct ADD COLUMN category2 VARCHAR;

UPDATE product_distinct
SET category2 =
CASE WHEN category = 'AMERICAN CORDIALS & LIQUEURS' THEN 'AMERICAN CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED DISTILLED SPIRITS SPECIALTY' THEN 'IMPORTED DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'NEUTRAL GRAIN SPIRITS FLAVORED' THEN 'NEUTRAL GRAIN SPIRITS'
	WHEN category = 'AMERICAN DISTILLED SPIRITS SPECIALTY' THEN 'AMERICAN DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'AMERICAN VODKAS' THEN 'AMERICAN VODKA'
	WHEN category = 'COCKTAILS /RTD' THEN 'COCKTAILS / RTD'
	WHEN category = 'COCKTAILS/RTD' THEN 'COCKTAILS / RTD'
	WHEN category = 'FLAVORED GINS' THEN 'FLAVORED GIN'
	WHEN category = 'IMPORTED CORDIALS & LIQUEURS' THEN 'IMPORTED CORDIALS & LIQUEUR'
	WHEN category = 'IMPORTED DISTILLED SPIRITS SPECIALTY' THEN 'IMPORTED DISTILLED SPIRIT SPECIALTY'
	WHEN category = 'TEMPORARY  & SPECIALTY PACKAGES' THEN 'TEMPORARY & SPECIALTY PACKAGES'
	WHEN category = 'TEMPORARY &  SPECIALTY PACKAGES' THEN 'TEMPORARY & SPECIALTY PACKAGES'
	WHEN category = 'TEMPORARY &  SPECIALTY PACKAGES' THEN 'TEMPORARY & SPECIALTY PACKAGES'
	WHEN category = 'SCHNAPPS - IMPORTED' THEN 'IMPORTED SCHNAPPS'
	WHEN category = 'AMARETTO - IMPORTED' THEN 'IMPORTED AMARETTO'
	WHEN category = 'SCHNAPPS - IMPORTED' THEN 'IMPORTED SCHNAPPS'
	WHEN category = 'IMPORTED VODKA - CHERRY' THEN 'IMPORTED VODKA'
	WHEN category = 'IMPORTED VODKA - MISC' THEN 'IMPORTED VODKA'
	WHEN category = 'MISC. IMPORTED CORDIALS & LIQUEURS' THEN 'IMPORTED CORDIALS & LIQUEUR'
	ELSE category
	END

-- remove category column as its no longer needed
ALTER table product_distinct
DROP COLUMN category

-- import category table with category_groupings to database
CREATE TABLE categories (
    category VARCHAR,
    category_group VARCHAR)
	
COPY categories FROM 'C:\Users\Public\liquor categories2.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- insert new Categorygroup column in the product_distinct table
ALTER table product_distinct
ADD column category_group varchar

UPDATE product_distinct
SET category_group = s.category_group
FROM categories AS s
WHERE product_distinct.category = s.category

--delete categories table. INNER JOIN update completed. 
DROP table categories

-- add the composite primary key constraint
ALTER TABLE price
ADD CONSTRAINT pk_price PRIMARY KEY (product_id, date);


-- Product_id and date are the primary keys, so each of these rows should be unique. 
-- delete duplicates
DELETE FROM price
WHERE (product_id, date, state_bottle_price) IN (
SELECT product_id, date, MIN(state_bottle_price)
    FROM priceys
    GROUP BY product_id, date
    HAVING COUNT(*) > 1
	)
	
-- convert data type 
ALTER TABLE orders
ALTER COLUMN date TYPE date USING to_date(date,'MM/DD/YYYY')


-- create county city population table
CREATE TABLE city (
    city VARCHAR,
	county VARCHAR,
	year int,
	population int)
	
ALTER TABLE city
ADD CONSTRAINT pk_city PRIMARY KEY (city, county, year) 
	
-- import table data
COPY city FROM 'C:\Users\Public\county and city population.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

--SET foreign key constraints
ALTER TABLE orders
ADD CONSTRAINT fk_store_id FOREIGN KEY(store_id) REFERENCES store(store_id)

ALTER TABLE orders
ADD CONSTRAINT fk_product_id FOREIGN KEY(product_id) REFERENCES product(product_id)

ALTER TABLE orders
ADD CONSTRAINT fk_product_id_date FOREIGN KEY(product_id, date) REFERENCES prices(product_id, date)

ALTER TABLE orders
ADD CONSTRAINT fk_vendor_id FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id)

ALTER TABLE product
ADD CONSTRAINT fk_vendor_id FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id)

--foreign key setting not working
--ALTER TABLE store
--ADD CONSTRAINT fk_country FOREIGN KEY(county) REFERENCES city(county)


-- create new prices table with missing product_id, date values from orders table. 
CREATE TABLE prices AS (
WITH CTE AS(
SELECT * FROM price
UNION
(SELECT DISTINCT o.product_id, o.date, o.pack_size, o.bottle_ml, o.state_bottle_cost,o.state_bottle_price
FROM orders as o
LEFT JOIN price as p
ON o.product_id = p.product_id AND o.date = p.date
WHERE p.product_id IS NULL
ORDER BY product_id))

SELECT product_id, date, max(pack_size) as pack_size, max(bottle_ml) as bottle_ml, max(state_bottle_cost) as state_bottle_cost ,max(state_bottle_price) as state_bottle_price
FROM CTE
GROUP BY product_id, date
ORDER BY product_id, date)

---------------------------------------------------------------------------
--here was looking at analyzing licensing fee and retail_sqft . 
-- decided to CREATE a LICENSING table instead of store_sqft table. 
-- make sure the store names are consistent. eliminate redundant columns.
-- retail_sqft column belongs in STORE table while licensing cost will be alongside licensing_ID
-- how to: alter STORE table add column retail_sqft then 
--can always create views or CTE if needed to re-use the same JOIN queries often

--add table column then use update set command
alter table A add column3 [yourdatatype];

update A set column3 = (select column3 from B where A.Column1 = B.Column2) 
  where exists (select column3 from B where A.Column1 = B.Column2)

--Create store_size tabe
CREATE TABLE store_sqft(
	store_id int PRIMARY KEY,
	store VARCHAR,
	zip_code int,
	county VARCHAR,
	license_id VARCHAR,
	license_cost decimal,
	retail_sqft decimal
	)
	
-- import table data
COPY store_sqft FROM 'C:\Users\Public\Iowa_store_squarefootage.csv'
DELIMITER ','
CSV HEADER
ENCODING 'UTF8';

-- 
SELECT * from store as s
LEFT JOIN store_sqft as f
ON s.store_id = f.store_id
WHERE s.zip_code != f.zip_code

UPDATE store

SELECT * from store as s
LEFT JOIN store_sqft as f
ON s.store_id = f.store_id



select * from store
where store ilike '%station%'
