use analysisproject;
SET SQL_SAFE_UPDATES = 0;
alter table `gold.fact_sales` rename to gold_fact_sales ;
ALTER TABLE `gold.report_products` RENAME TO gold_report_products;

select * from gold_report_customers;
select * from gold_fact_sales;

-- ANALYZE sales over time 
select year(order_date)as year_order_date,
 month(order_date)as month_order_date,
sum(price) as sum_price from gold_fact_sales
group by year_order_date,month_order_date
having year_order_date is not null
order by year_order_date,month_order_date ;

-- cummulative analyze

select year_month_order_date,
sum_price,
avg_price,
sum(sum_price) over( PARTITION BY year_order_date order by year_month_order_date) as trend_analysis,
avg(avg_price) over( PARTITION BY year_order_date order by year_month_order_date) as avg_analysis
from 
(select date_format(order_date,'%Y-%m')as year_month_order_date,
 year(order_date)as year_order_date,
 avg(price) as avg_price ,
 sum(price) as sum_price from gold_fact_sales
group by year_month_order_date,year_order_date
having year_order_date is not null
) t;
-- performance analysis
with cte1 as 
(
 select year(f.order_date) as year_order_date,
 sum(f.sales_amount) as sales_sum,
 g.product_name as p_name
 from gold_fact_sales f
 left join gold_report_products g 
 on f.product_key = g.product_key
 GROUP BY year_order_date,product_name
 having year_order_date is not null
 )
 select p_name,
 year_order_date,
 sales_sum,
 avg(sales_sum) over(partition by p_name) as avg_sales,
 sales_sum-avg(sales_sum) over(partition by p_name) as avg_diff,
 case when sales_sum-avg(sales_sum) over(partition by p_name) >0 then 'above average'
 when sales_sum-avg(sales_sum) over(partition by p_name) <0 then 'below average' 
 else 'equal'
 end as avg_change,
 lag(sales_sum) over(partition by p_name order by year_order_date) as sales_diff,
 case when  lag(sales_sum) over(partition by p_name order by year_order_date) > sales_sum then 'decrease'
 when  lag(sales_sum) over(partition by p_name order by year_order_date) < sales_sum then 'increase'
 else 'no change' end as laag
 from cte1
 order by p_name, year_order_date
 ;
 -- part to whole analyze
 with cte2 as(
 select p.subcategory as scat,
 sum(f.sales_amount) as sales_sum 
 from gold_fact_sales f
 left join gold_report_products  p 
 on p.product_key = f.product_key
 group by p.subcategory)
 select scat,sales_sum,
 sum(sales_sum) over() as total_sales ,
  concat(round(sales_sum/sum(sales_sum) over(),2)*100,'%') as find_p
 from cte2
 order by find_p desc

with cte3 as(
 select p.category as cat,
 sum(f.sales_amount) as sales_sum 
 from gold_fact_sales f
 left join gold_report_products  p 
 on p.product_key = f.product_key
 group by p.category)
 select cat,sales_sum,
 sum(sales_sum) over() as total_sales ,
concat(round(sales_sum/sum(sales_sum) over()*100,2),'%') as find_p
 from cte3
 order by find_p desc

with cte4 as (
select product_key ,
cost,
case when cost <100 then 'below 100'
when cost between 100 and 500 then '100-500'
when cost between 500 and 1000 then '500-1000'
else 'above 1000'
end as costdimension
 from gold_report_products)
 select costdimension ,count(product_key) from cte4
 group by costdimension
 ;
	 with cte5 as (
	select c.customer_key as customer_key,
	sum(f.sales_amount), 
	round(datediff(max(order_date),min(order_date))/30) as lifespan,
	case when sum(f.sales_amount) >5000 and round(datediff(max(order_date),min(order_date))/30) >12 THEN 'VIP'
	when sum(f.sales_amount) between 1000 AND 5000 and round(datediff(max(order_date),min(order_date))/30) >12 THEN 'GOLD'

	ELSE 'NEW'
	END AS CATSUITE
	 from gold_report_customers  c 
	left join gold_fact_sales f on f.customer_key=c.customer_key
	group by c.customer_key)
	select CATSUITE,COUNT(customer_key) FROM CTE5
	group by catsuite

 
 
 
