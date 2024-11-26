---- query to print top 5 cities with highest spends and their percentage contribution of total credit card spends 

with city_spend_cte as
(select top 5 city,sum(cast(amount as bigint)) as total_city_sales
from credit_card_transactions
group by city
order by total_city_sales desc),
tas as (select sum(cast(amount as bigint)) as total_amount_spent
from credit_card_transactions )

select city,(ct.total_city_sales*1.0/t.total_amount_spent)*100 as percent_spent
from city_spend_cte ct,tas t
order by total_city_sales desc

-----query to print highest spend month and amount spent in that month for each card type

with mos_cte as
(SELECT card_type,DATEPART(MONTH,transaction_date) as month_no,sum(amount) as monthly_spent
from credit_card_transactions
group by card_type,DATEPART(MONTH,transaction_date)),
srn as(select *,
ROW_NUMBER() over(partition by card_type order by monthly_spent desc) as rn
from mos_cte)
select *
from srn
where rn = 1

-------this code calculates the cumulative spend for each row

with running_total as
(select *,
SUM(amount) over(partition by card_type order by amount) as cumulitive_spent
from credit_card_transactions),

rn_cte as(select *,
ROW_NUMBER() over(partition by card_type order by amount) as rn
from running_total
where cumulitive_spent >= 1000000 )

select * 
from rn_cte
where rn =1;


----query to find city which had lowest percentage spend for gold card type

with gca_cte as
(select city,sum(cast(amount as bigint)) as gold_card_amount
from credit_card_transactions
where card_type = 'Gold'
group by city),

tcs as (select sum(cast(gold_card_amount as bigint)) as total_gold_card_spent
from gca_cte)

select top 1 city ,(g.gold_card_amount*1.0/t.total_gold_card_spent)*100 as percentage_spent
from gca_cte g, tcs t
order by percentage_spent


------query to print 3 columns:  city, highest_expense_type , lowest_expense_type

with ce_cte as
(select city,exp_type,sum(amount) as s_amount
--ROW_NUMBER() over(partition by city order by city)
from credit_card_transactions
group by city,exp_type),

lr as (select *,
rank() over(partition by city order by s_amount asc) as low_rank
from ce_cte),

hr as ( select  *,
rank() over(partition by city order by s_amount desc) as high_rank
from ce_cte)

select l.city,l.exp_type as lowest_expense,r.exp_type as highest_expense
from lr l
left join hr r on l.city = r.city 
where l.low_rank = 1 and r.high_rank = 1

-----query to find percentage contribution of spends by females for each expense type

with fs_cte as
(select exp_type,sum(amount) as f_sales
from credit_card_transactions
where gender = 'F'
group by exp_type),

ts as (select exp_type,sum(cast(amount as bigint)) as t_sales
from credit_card_transactions
group by exp_type)

select f.exp_type, (f_sales*1.0/t_sales)*100 as percentage_sale
from fs_cte f,ts t
where f.exp_type = t.exp_type
