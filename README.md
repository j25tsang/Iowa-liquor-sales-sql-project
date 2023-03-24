# Iowa-liquor-sales-sql-project
<pre>
CSV Dataset sources imported to PostgresSQL database: 
Iowa Liquor sales data https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy
Iowa Liquor products data https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Products/gckp-fe7r </pre>

<pre>
Business challenge
An aspiring store owner in Iowa wants to open up a new store at a prime location that would enable them to maximize sales and profits. 
They also need to figure out the optimal product catalog mix to minimize overstocking and maximize sales per square foot. 

My objective is to support a hypothetical liquor store owner based in Iowa in their endeavor to expand to new locations across the state. 
Additionally, I plan to provide general recommendations for suitable locations to the business owner 
as well as provide product recommendations based on demand an sales trends

Questions to help define objective 
Prime store location factors
•	Population density
•	Demographics of the area
•	Proximity to other businesses
•	Accessibility and visibility
•	Rent and lease terms

Initial exploratory questions
•	What are the most popular brands of liquor in Iowa?
•	What is the trend of liquor sales in Iowa?
•	What are the most popular types of liquor sold in Iowa?
•	What is the average price of liquor sold in Iowa?
•	What is the total revenue generated by liquor sales in Iowa?

Key performance indicators
•	Sales per square foot is a key KPI in measuring a store’s efficiency in driving sales in the space allotted to them
•	Maximizing average order value and average basket size 

Data processing
As the data is denormalized, the first step after importing the dataset to a local PostgresSQL database
was to normalize the dataset so that querying the data would be more streamlined and improve querying speed.

Data cleaning
As there were a lot of missing fields, null values, and inconsistency across table relationships, 
significant time was spent cleaning the data before conducting  the data analysis. 

</pre>

