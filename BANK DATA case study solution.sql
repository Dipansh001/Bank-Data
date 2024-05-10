select * from customer_nodes
select * from customer_transactions
select *from regions


---------------A. Customer Nodes Exploration-------------


--1-How many unique nodes are there on the Data Bank system--
select count(distinct node_id) as Unique_node
from customer_nodes;

/*2-What is the number of nodes per region?*/
select  r.region_id,region_name,count(distinct node_id) as nodes, 
count(node_id) as total_nodes
from customer_nodes as c
inner join regions as r
on  r.region_id= c.region_id
group by r.region_id,r.region_name
order by r.region_id


--3-How many customers are allocated to each region?--
select r.region_id,r.region_name, count(distinct customer_id) as total_customer
from customer_nodes as c
inner join regions as r
on c.region_id = r.region_id
group  by r.region_id,region_name
order by r.region_id

--4-How many days on average are customers reallocated to a different node?--
select AVG(DATEDIFF(DD,start_date, end_date)) as Avg_days
from customer_nodes
where end_date != '99991231' ;



--5-What is the median, 80th and 95th percentile for this same reallocation days metric for each region?--
with 
 diff_data
 as 
 (
 select c.customer_id,c.region_id,r.region_name,
 DATEDIFF(D,c.start_date,c.end_date) as diff
 from customer_nodes as c
 inner join regions as r
 on  c.region_id= r.region_id
 where end_date != '99991231'
 )

select distinct region_id,region_name,

PERCENTILE_CONT(0.5) WITHIN GROUP(order by diff)  over (partition by region_name) as Median,
PERCENTILE_CONT(0.8) WITHIN GROUP (order by diff) over(partition by region_name) as percentile_80,
PERCENTILE_CONT(0.95) WITHIN GROUP(order by diff) over(partition by region_name ) as percentile_95

from diff_data
order by region_id;


							-------B. Customer Transactions-----
--1-What is the unique count and total amount for each transaction type?--
select txn_type as Transaction_Type,
count(txn_type) as Unique_code,
sum(txn_amount) as Total_amount
from customer_transactions
group by txn_type
order by txn_type

--2-What is the average total historical deposit counts and amounts for all customers?--
 with
      historical
as 
(
select 
		c.customer_id,
		t.txn_type,
			COUNT(t.txn_type) as Total_count,
			avg(t.txn_amount) as Total_amount
				from customer_transactions as t
				inner join customer_nodes as c on c.customer_id=t.customer_id 
				inner join regions as r on r.region_id=c.region_id
				group by c.customer_id,t.txn_type
				)
				
					select avg(Total_count) as  Hist_count,
					avg(total_amount) as Total_amount
					from historical
					where txn_type ='deposit';


		  ------METHOD 3------

select 
 c.customer_id,
 DATEPART(M,t.txn_date) as months,
 DATENAME(M,t.txn_date) as month_name,
 count(t.txn_type) total_customer
from customer_transactions as t
inner join customer_nodes as c on c.customer_id=t.customer_id
inner join regions as r on r.region_id =c.region_id
group by c.customer_id , DATEPART(M,t.txn_date),DATENAME(M,t.txn_date);





--3-For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal 
--in a single month?-

											--METHOD 1--
with 
	historical
	as 
	   (
		select 
		c.customer_id,
		DATEPART(M,t.txn_date) as months,
		datename(M,t.txn_date) as month_name,
		count(txn_type) as total_tran
			from customer_transactions as t
			inner join customer_nodes as c on c.customer_id=t.customer_id
			inner join regions as r on r.region_id = c.region_id
			group by c.customer_id,DATEPART(M,t.txn_date),datename(M,t.txn_date) 
	),
     deposits
	   as
	   ( 
	    select 
		    c.customer_id,
			DATEPART(M,t.txn_date) as months,
			datename(M,t.txn_date) as month_name,
			SUM(case when t.txn_type = 'deposit' then 1 else 0 end) as deposits
			from customer_transactions as t
			inner join customer_nodes as c on c.customer_id=t.customer_id
			inner join regions as r on r.region_id = c.region_id
			group by c.customer_id,DATEPART(M,t.txn_date),datename(M,t.txn_date)

	),
	 purchase
	  as  
	  ( 
	    select 
		    c.customer_id,
			DATEPART(M,t.txn_date) as months,
			SUM(case when  t.txn_type='purchase' then 1 else 0 end) as purchase
			from customer_transactions as t
			inner join customer_nodes as c on c.customer_id=t.customer_id
			inner join regions as r on r.region_id = c.region_id
			group by c.customer_id,DATEPART(M,t.txn_date),datename(M,t.txn_date)
),
 Withdrawal
        as  
	  ( 
	    select 
		    c.customer_id,
			DATEPART(M,t.txn_date) as months,
			SUM(case when  t.txn_type='withdrawal' then 1 else 0 end) as Withdrawal
			from customer_transactions as t
			inner join customer_nodes as c on c.customer_id=t.customer_id
			inner join regions as r on r.region_id = c.region_id
			group by c.customer_id,DATEPART(M,t.txn_date),datename(M,t.txn_date)
),

demo1
as 
   (
     select 
			h.customer_id,h.months,h.month_name,h.total_tran,d.deposits,purchase,w.Withdrawal
			from historical as h
			inner join purchase as p on p.customer_id=h.customer_id and p.months=h.months
			inner join deposits as d on d.customer_id=p.customer_id and d.months=h.months
			inner join Withdrawal as w on w.customer_id =h.customer_id and w.months=h.months

		)
		
		select months,month_name,
		  count(customer_id) cust_count
		  from demo1 
		  where deposits >=1 and (purchase >=1 or Withdrawal>=1)
		  group by months,month_name
		  order by months


										---METHOD-2---

WITH cte as 
(
select customer_id,
DATEPART(MONTH,txn_date) as Month_ID,
sum(case when txn_type= 'deposit' then 1 else 0  end)  as Deposits,
sum(case when txn_type = 'purchase' then 1 else 0 end) as Purchase,
sum(case when txn_type = 'withdrawal' then 1 else 0 end) as withdrawal
from customer_transactions
group by customer_id,DATEPART(month,txn_date))

select month_id,count(*) as  Total_cust
from cte 
where (Deposits >1 and Purchase=1 or withdrawal=1)
group by month_id;


--4-What is the closing balance for each customer at the end of the month?--
  with CTE as
  (
  select customer_id,
  DATEPART(M,txn_date) as Month_id,
  DATENAME(MONTH,txn_date) as Month_Name,
  sum(case when txn_type ='deposit' then  txn_amount else 0 end) as deposits,
  sum(case when txn_type = 'purchase' then -txn_amount else 0 end) as purchase,
  sum(case when txn_type = 'withdrawal' then -txn_amount else 0 end) as withdrawal
  from  customer_transactions 
  group by customer_id,DATEPART(M,txn_date),DATENAME(MONTH,txn_date)  
  )
  select 
  Customer_id,Month_id,Month_Name,
  sum(deposits+purchase+withdrawal) 
  over(partition by customer_id order by customer_id,month_id,month_name) as Balance,
  deposits+purchase+withdrawal as New_Balance
  from cte

  --5-What is the percentage of customers who increase their closing balance by more than 5% ?--
 WITH Month_Tran AS (
    SELECT
        customer_id,
        MONTH(txn_date) AS End_date,
        SUM(
            CASE
                WHEN txn_type IN ('Withdrawal', 'Purchase') THEN -txn_amount
                ELSE txn_amount
            END
        ) AS transactions
    FROM customer_transactions
    GROUP BY customer_id, MONTH(txn_date)
),
closing_balance AS (
    SELECT
        customer_id,
        end_date,
        ISNULL(SUM(transactions) OVER (PARTITION BY customer_id ORDER BY end_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS closing_balance
    FROM Month_Tran
),
pct_increase AS (
    SELECT
        customer_id,
        end_date,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date) AS prev_closing_balance,
        (closing_balance - LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date)) / NULLIF(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY end_date), 0) AS pct_inc
    FROM closing_balance
)
SELECT 
    CAST(100.0 * COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM customer_transactions) AS FLOAT) AS pct_customers
FROM pct_increase
WHERE pct_inc > 0.05;

	
	
	
	
	
	






