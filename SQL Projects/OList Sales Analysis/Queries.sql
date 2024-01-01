#Duplicated rows check in order_items tables
WITH
cte AS (SELECT *,
               DENSE_RANK() OVER (PARTITION BY order_id,order_item_id ORDER BY seller_id) AS rnk
		FROM olist.order_items)
SELECT * 
FROM cte 
WHERE rnk>1;
#There are no duplicated rows in the order_items table.


#Extracting Datatypes of all columns in the "customers" table.
DESCRIBE olist.customers;


#Extracting Datatypes of all columns in the "orders" table.
DESCRIBE olist.orders;


#Extracting Datatypes of all columns in the "payments" table.
DESCRIBE olist.payments;


#Extracting Datatypes of all columns in the "payments" table.
DESCRIBE olist.order_items;


#Number of records in order_items Table
SELECT COUNT(*) as total_rows
FROM olist.order_items;
#There are 112650 records at ordered items granularity


#The time range between which the orders were placed.
SELECT MIN(order_purchase_timestamp) AS first_time_ordered,
       MAX(order_purchase_timestamp) AS last_time_ordered 
FROM olist.orders;
#The dataset ranges between 2016-09-04 and 2018-10-17.


#Creating a View for each of the months between the first and last order date 
CREATE VIEW yearmonth AS 
WITH 
RECURSIVE yr as (SELECT MIN(Year(order_purchase_timestamp)) AS 'year' FROM olist.orders
                UNION ALL
                SELECT year+1 
                FROM yr 
                WHERE year<=(SELECT MAX(Year(order_purchase_timestamp)) FROM olist.orders)),
          mon AS (SELECT 01 AS month
                                                 UNION ALL
                                                 SELECT month+1 FROM mon WHERE month<=11),
yearmonthtab as (SELECT CONCAT(year," ",month," ","1") AS yearmonth 
               FROM yr CROSS JOIN mon)
SELECT date_format(str_to_date(yearmonth,"%Y %m %d"),"%Y-%m") as yearmonth from yearmonthtab ORDER BY 1;


#Unique count of Cities & States of customers who ordered during the given period.
SELECT COUNT(DISTINCT customer_city) AS num_cities,
	   COUNT(DISTINCT customer_state) AS num_states
FROM olist.orders o JOIN olist.customers c ON o.customer_id=c.customer_id;
#Within the time period, orders were placed from 4119 distinct cities and 27 distinct states.


#Number of states and cities in Brazil where olist is not operating 
WITH
cte AS (SELECT DISTINCT customer_city
        FROM olist.orders o JOIN olist.customers c ON o.customer_id=c.customer_id),
cte1 AS (SELECT DISTINCT customer_state
         FROM olist.orders o JOIN olist.customers c ON o.customer_id=c.customer_id),
cte2 AS (SELECT COUNT(DISTINCT geolocation_city) 
         FROM olist.geolocation 
         WHERE geolocation_city NOT IN (SELECT customer_city FROM cte)),
cte3 AS (SELECT COUNT(distinct geolocation_state) 
         FROM olist.geolocation
         WHERE geolocation_state NOT IN (SELECT customer_state FROM cte1))
SELECT (SELECT * FROM cte2) AS num_cities,
       (SELECT * FROM cte3) AS num_states;
#There are 3942 Cities from where Olist has never received a single order but it does operate in every single State.


#Trend in the no. of orders placed over the past years
WITH 
cte AS (SELECT DATE_FORMAT(order_purchase_timestamp,"%Y-%m") AS yearmonth,
               COUNT(*) AS order_count
        FROM olist.orders
        GROUP BY 1
        ORDER BY 1)
SELECT ym.yearmonth,
       IFNULL(order_count,0) AS order_count 
FROM olist.yearmonth ym LEFT JOIN cte c ON c.yearmonth=ym.yearmonth
ORDER BY 1;
/* 
-- There's a significant increase in the number of orders placed over different months. Since the beginning of 2018, the number of orders
               placed has become pretty consistent with more than 6000 orders placed every month.
-- There were zero orders placed during November 2016. 
*/

#Monthly seasonality seen in number of orders getting placed
WITH
cte AS (SELECT DATE_FORMAT(order_purchase_timestamp,"%b") AS Month,
               DATE_FORMAT(order_purchase_timestamp,"%m") as month_num,
               COUNT(*) AS order_count
        FROM olist.orders
        GROUP BY 1,2
        ORDER BY 2)
SELECT Month,
order_count
FROM cte;
/*
The Company received a fairly lower number of orders in between the months of
         September and December.
Also this seems a little biased result because we don't have the data for November 2018 and December 2018
Also we don't have any data for the first three months of 2016.
But the Company sales remained pretty high during the first 8 months of the year
*/


#Is there some time preferred by the Brazilian customers for placing their orders?
WITH
cte as (SELECT HOUR(order_purchase_timestamp) AS "hour",
               COUNT(*) as order_count
        FROM olist.orders
        GROUP BY 1
        ORDER BY 1)
SELECT CASE WHEN hour<=6 THEN "Dawn"
            WHEN hour<=12 THEN "Mornings"
            WHEN hour<=18 THEN "Afternoon"
            WHEN hour<=23 THEN "Night"
            END AS Time_of_Day,
       SUM(order_count) AS order_count
FROM cte
GROUP BY 1
ORDER BY 2 DESC;
/*
Most of the Olist's customers prefer shopping during the Afternoon. But there's a fair share
        of products sold during Nights and Mornings as well . The sales fall steeply during the Dawn
        which sounds pretty obvious.
*/


#Month over month number of orders placed in each state
WITH 
cte as (SELECT customer_state AS State,
               DATE_FORMAT(order_purchase_timestamp,"%Y-%m") as year_mon,
               COUNT(*) AS orders_count
        FROM olist.orders o JOIN olist.customers c ON o.customer_id=c.customer_id
        GROUP BY 1,2 
        ORDER BY 1,2),
cte1 AS (SELECT DISTINCT geolocation_state AS State 
         FROM olist.geolocation),
neww AS (SELECT c1.State,
                ym.yearmonth 
         FROM cte1 c1 CROSS JOIN olist.yearmonth ym)
SELECT n.State,
       n.yearmonth,
       IFNULL(c.orders_count,0) AS orders_count 
FROM neww n LEFT JOIN cte c ON n.State=c.State AND n.yearmonth=c.year_mon 
ORDER BY 1,2;
/*
The state of São Paulo(SP) has shown the most significant rise in the number of orders
         getting placed over the period of time.

The states of Rio de Janeiro(RJ) and Minas Gerais(MG) have shown intermediate rise in the
         number of orders being placed .

The states of Rio Grande do Sul(RS), Santa Catarina(SC), Paraná(PR), Bahia(BA) also showed
         satisfactory signs of an increase in the number of orders being placed during this time
*/
        
        
#Distribution of customers across States
SELECT customer_state AS State,
       COUNT(distinct customer_id) as number_of_customers
FROM olist.customers
GROUP BY 1
ORDER BY 2 DESC;
/*
The state of São Paulo(SP) accounts for the largest number of customers
         across all the states of Brazil.

The states of Rio de Janeiro(RJ) and Minas Gerais(MG) too have a fair share
         of the number of customers.

The state of Roraima(RR) accounts for the lowest number of customers across
             all the states of Brazil.
*/


#Average number of days between the date of order getting placed and the expected delivery date:
SELECT customer_state,
       ROUND(AVG(DATEDIFF(order_estimated_delivery_date,order_purchase_timestamp)),2) AS ordered_expected_difference
FROM olist.orders o JOIN olist.customers c ON o.customer_id=c.customer_id
GROUP BY 1
ORDER BY 2;
/*
SP has the highest number of customers and the average expected differenc between the order date and
             the date of delivery is fairly lower than all the other states.

RR has the lowest number of customers and the average expected difference
             between the order date and the date of delivery is the highest.
*/


#Month over month change in the percentage of the sales
WITH 
cte AS (SELECT DATE_FORMAT(order_purchase_timestamp,"%Y-%m") as yearmonth,
               SUM(payment_value) AS Total_amount  
        FROM olist.payments p JOIN olist.orders o ON p.order_id=o.order_id
        GROUP BY 1), 
cte1 AS (SELECT ym.yearmonth,
                IFNULL(Total_amount,0) AS Total_amount
         FROM olist.yearmonth ym LEFT JOIN cte c ON ym.yearmonth=c.yearmonth), 
cte2 AS (SELECT yearmonth,
                Total_amount,
                LAG(Total_amount) OVER (ORDER BY yearmonth ASC) AS prev_amount
         FROM cte1 
         ORDER BY yearmonth), 
cte3 AS (SELECT yearmonth,
                CASE WHEN prev_amount!=0 THEN
                (((Total_amount-prev_amount)/prev_amount)*100.0) ELSE NULL END AS percentage_change
         FROM cte2) 
SELECT yearmonth,
        concat(percentage_change,"%") AS percentage_change
FROM cte3;
/*
The company’s sales took off during the first month of 2017 and kept increasing.

There were a few months where the sales was not that happening compared to the previous month. But we do not see a lot of them.
*/
	
	
#Total & Average value of order price for each state
WITH
net_price AS (SELECT order_id,
                     SUM(price) AS price
              FROM olist.order_items
              GROUP BY 1)
SELECT customer_state AS State,
                    ROUND(SUM(price),2) AS Total_Price,
                    ROUND(AVG(price),2) AS Average_Price
FROM net_price p JOIN  olist.orders o ON p.order_id=o.order_id
                 JOIN olist.customers c ON c.customer_id=o.customer_id
GROUP BY 1;
/*
The average price per order is the highest for Paraíba(PB) and lowest for the state of São Paulo.

The total order price for the state of São Paulo is simply much much higher than any of the other states and 
             because of a higher count of orders, the average did come down. So,it won’t be fair to draw any conclusion.
*/


#Total & Average value of order freight for each state
WITH
cte AS (SELECT oi.order_id,
               SUM(freight_value) AS Net_freight
        FROM olist.order_items oi JOIN olist.orders o ON o.order_id=oi.order_id
        GROUP BY 1)
SELECT customer_state AS State,
                    ROUND(SUM(Net_freight),2) as Total_freight,
                    ROUND(AVG(Net_freight),2) AS Average_freight
             FROM cte c join olist.orders o ON c.order_id=o.order_id join olist.customers cu ON o.customer_id=cu.customer_id
             GROUP BY 1
             ORDER BY 1;
/*
The state of Roraima(RR) holds the highest average freight value and we have already seen that the state is having the lowest share of customers.

The average freight value is lowest for the state of São Paulo and the state tops the chart of the number of customers.
*/


#Percentage of orders delivered on time
WITH
cte as (SELECT order_id, 
               DATEDIFF(order_delivered_customer_date,order_purchase_timestamp) AS time_to_deliver, 
               DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date) AS diff_estimated_delivery, 
               CASE WHEN DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date)<0 THEN "Delayed"  
                    WHEN DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date)=0 THEN "On-Time" 
                    WHEN DATEDIFF(order_estimated_delivery_date,order_delivered_customer_date)>0 THEN "Early" 
                    END AS on_time_delivery 
       FROM olist.orders
       WHERE order_delivered_customer_date IS NOT NULL),
cte1 AS (SELECT on_time_delivery,
                COUNT(*) AS order_count 
         FROM cte 
         GROUP BY 1
         ORDER BY 2 DESC),
cte2 AS (SELECT *,
                SUM(order_count) OVER () AS total_orders 
         FROM cte1)
SELECT on_time_delivery,order_count,
       concat(ROUND(order_count/total_orders *100.0,2),"%") AS Percent_out_of_total
FROM cte2;
/*
Most of the orders(approximately 93.2%) were either delivered early or they were delivered on-time
*/


#Top 5 states with the highest & lowest average freight value
WITH
cte AS (SELECT order_id,
               SUM(freight_value) AS Net_freight
        FROM olist.order_items
        GROUP BY 1),
required as (SELECT customer_state AS State,
                    SUM(Net_freight) as Total_freight,
                    AVG(Net_freight) AS Average_freight
             FROM cte c JOIN olist.orders o ON c.order_id=o.order_id
                        JOIN olist.customers cu ON o.customer_id=cu.customer_id
             GROUP BY 1
             ORDER BY 1),
cte1 AS (SELECT State,
                Average_freight,
                DENSE_RANK() OVER (ORDER BY Average_freight DESC) AS rnk,
                DENSE_RANK() OVER (ORDER BY Average_freight ASC) AS rnk1
         FROM required
         ORDER BY rnk)
SELECT State,
       Average_freight,
       CASE WHEN rnk<=5 THEN "Top 5" WHEN rnk1<=5 THEN "Bottom 5" END AS TOP_or_Bottom
FROM cte1
WHERE rnk<=5 OR rnk1<=5
ORDER BY rnk DESC ;
/*
The state of São Paulo(SP) has the lowest average freight value.

The state of Roraima(RR) has the highest average freight value.
*/


#Top 5 states with the highest & lowest average delivery time
WITH
cte AS (SELECT order_id,
               customer_id,
               DATEDIFF(order_delivered_customer_date,order_purchase_timestamp) AS time_to_deliver
        FROM olist.orders
        WHERE order_delivered_customer_date IS NOT NULL),
cte1 AS (SELECT customer_state,
                AVG(time_to_deliver) as Average_delivery_time
         FROM cte c JOIN olist.customers cu ON c.customer_id=cu.customer_id  
         GROUP BY 1),
cte2 AS (SELECT *,
                DENSE_RANK() OVER (ORDER BY Average_delivery_time DESC) AS rnk,
                DENSE_RANK() OVER (ORDER BY Average_delivery_time ASC) AS rnk1
         FROM cte1)
SELECT customer_state,
       Average_delivery_time,
       CASE WHEN rnk<=5 THEN "Top 5" WHEN rnk1<=5 THEN "Bottom 5" END AS Top_or_Bottom
FROM cte2
WHERE rnk<=5 OR rnk1<=5
ORDER BY rnk DESC;
/*
The state of São Paulo(SP) is being provided with the quickest delivery in terms of the average number of days between the order date
                              and the date when the order gets delivered.

The state of Roraima(RR) receives slowest deliveries in terms of the average number of days between the order date
                              and the date when the order gets delivered.
*/


#Top 5 states where the order delivery is really fast when compared to the estimated date of delivery.
WITH
cte AS (SELECT order_id,
               customer_id,
               ROUND(DATEDIFF(order_delivered_customer_date,order_estimated_delivery_date),6) AS diff_estimated_delivery
        FROM olist.orders
        WHERE order_status="delivered"),
cte1 AS (SELECT customer_state AS State,
                AVG(diff_estimated_delivery) AS Average_difference
         FROM cte c JOIN olist.customers cu ON c.customer_id=cu.customer_id
         GROUP BY 1),
cte2 AS (SELECT State,
                Average_difference,
                DENSE_RANK() OVER (ORDER BY Average_difference ASC) AS rnk
         FROM cte1)
SELECT State,
       ROUND(ABS(Average_difference),2) AS Average_days_before_estimation
FROM cte2
WHERE rnk<=5
ORDER BY rnk ;
/*
The prediction algorithm used by the company to predict delivery dates hasn't been working so efficiently.

The lowest average difference is 7.95 days which looks pretty bad in terms of prediction.
*/


#Month over month number of orders placed using different payment types
WITH
cte AS (SELECT DISTINCT order_id,
               payment_type
        FROM olist.payments
        ORDER BY 1,2),
cte1 AS (SELECT order_id,
                GROUP_CONCAT(payment_type) AS payment_type
         FROM cte
         GROUP BY 1),
cte2 AS (SELECT payment_type,
                DATE_FORMAT(order_purchase_timestamp,"%Y-%m") AS yearmonth,
                COUNT(*) AS order_count
         FROM cte1 c1 JOIN olist.orders o ON c1.order_id=o.order_id
         WHERE payment_type!="not_defined"
         GROUP BY 1,2
         ORDER BY lower(payment_type),2)
SELECT ym.yearmonth AS yearmonth,
       c2.payment_type,
       IFNULL(order_count,0) AS order_count
FROM olist.yearmonth ym LEFT JOIN cte2 c2 ON ym.yearmonth=c2.yearmonth
ORDER BY 1,2;


#Most used payment method for each of the months
WITH
cte AS (SELECT DISTINCT order_id,
               payment_type
        FROM olist.payments
        ORDER BY 1,2),
cte1 AS (SELECT order_id,
                GROUP_CONCAT(payment_type) AS payment_type
         FROM cte
         GROUP BY 1),
cte2 AS (SELECT payment_type,
                DATE_FORMAT(order_purchase_timestamp,"%Y-%m") AS yearmonth,
                count(*) as order_count
                FROM cte1 c1 JOIN olist.orders o ON c1.order_id=o.order_id
                WHERE payment_type!="not_defined"
                GROUP BY 1,2
                ORDER BY lower(payment_type),2),
cte3 AS (SELECT ym.yearmonth AS yearmonth,
                c2.payment_type,
                IFNULL(order_count,0) AS order_count
         FROM olist.yearmonth ym LEFT JOIN cte2 c2 ON ym.yearmonth=c2.yearmonth
         ORDER BY 1,2),
cte4 AS (SELECT *,
                DENSE_RANK() OVER (PARTITION BY yearmonth ORDER BY order_count DESC) AS rnk
         FROM cte3)
SELECT yearmonth,
       payment_type,
       order_count
FROM cte4 
WHERE rnk=1 
ORDER BY 1;
/*
Credit Cards have always been the most preferred mode of payment for the customers.
*/


#Number of orders placed on the basis of the payment installments that have been paid.
WITH
cte AS (SELECT order_id,
               MAX(payment_sequential) OVER (PARTITION BY order_id) AS max_seq 
        FROM olist.payments)
SELECT max_seq AS payment_installments,
       COUNT(DISTINCT order_id) AS order_count 
FROM cte 
GROUP BY 1 
ORDER BY 1;
/*
Most of the orders placed are either no EMI ones or the ones where only the first installment of the EMI has been paid
*/



/* 
__Summary of Analysis__

Order Trends:
The number of orders has shown a significant increase over time, with a consistent high volume since the beginning of 2018.

Seasonal Trends:
There is a noticeable monthly seasonality, with lower order counts observed between September and December.

Order Placement Time:
Most Brazilian customers prefer placing orders during the afternoon, although there are also significant sales during mornings and nights.

Regional Analysis:
São Paulo (SP) has experienced a significant increase in the number of orders, followed by Rio de Janeiro (RJ) and Minas Gerais (MG).
São Paulo (SP) has the highest number of customers.

Cost Analysis:
São Paulo (SP) has the highest total order price, but the average order price is highest for Paraíba (PB).

Freight Analysis:
Roraima (RR) has the highest average freight value, while São Paulo (SP) has the lowest.

Delivery Time:
São Paulo (SP) experiences the quickest delivery times, while Roraima (RR) has the slowest.

On-Time Delivery:
Approximately 93.2% of orders are either delivered early or on time.

Payment Methods:
Credit cards are the most preferred payment method for customers.

EMI Payments:
Most orders are either not on EMI or have only the first installment paid.

Sales Performance:
The company's sales increased, with a few months showing a decrease compared to the previous month.

Fastest Delivery States:
São Paulo (SP) receives the fastest deliveries compared to the estimated delivery date.

Payment Type Analysis:
Credit cards consistently dominate as the most used payment method for each month.

Prediction Accuracy:
The prediction algorithm for delivery dates shows room for improvement, with an average difference of 7.95 days.


These insights provide a comprehensive overview of OList's operations in Brazil, covering aspects such as customer behavior, regional variations,
payment preferences, and delivery efficiency. The analysis can guide strategic decisions to enhance customer satisfaction and optimize business processes.
*/