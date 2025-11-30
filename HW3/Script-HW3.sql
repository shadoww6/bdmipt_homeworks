-- 1 запрос
select job_industry_category,
	   COUNT(*) as client_count
from customer
group by job_industry_category 
order by client_count desc;

-- 2 запрос
select EXTRACT(YEAR FROM TO_DATE(o.order_date, 'YYYY-MM-DD')) AS year,
	   extract(month from TO_DATE(o.order_date, 'YYYY-MM-DD')) as month,
	   c.job_industry_category,
	   SUM(p.list_price * oi.quantity) as total_revenue
from customer c
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id
join product_cor p on oi.product_id = p.product_id
where o.order_status = 'Approved'
group by EXTRACT(YEAR FROM TO_DATE(o.order_date, 'YYYY-MM-DD')),
	     extract(month from TO_DATE(o.order_date, 'YYYY-MM-DD')),
	     c.job_industry_category
order by year, month, c.job_industry_category;

-- 3 запрос
select p.brand,
	   COUNt(distinct o.order_id) as online_orders_count
from product_cor p
left join order_items oi on p.product_id = oi.product_id
left join orders o on oi.order_id = o.order_id 
					  and o.online_order = true
					  and o.order_status = 'Approved'
left join customer c on o.customer_id = c.customer_id
					 and c.job_industry_category = 'IT'
group by p.brand 
order by online_orders_count DESC, p.brand;

-- 4 запрос через группировку
select c.customer_id,
       c.first_name,
       c.last_name,
       SUM(oi.quantity * p.list_price) as total_revenue,
       MAX(oi.quantity * p.list_price) as max_order,
       MIN(oi.quantity * p.list_price) as min_order,
       COUNT(distinct o.order_id) as orders_count,
       AVG(oi.quantity * p.list_price) as avg_order
from customer c
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id
join product_cor p on oi.product_id = p.product_id
group by c.customer_id, c.first_name, c.last_name
order by total_revenue desc, orders_count DESC;

-- 4 запрос через оконные
select distinct c.customer_id,
       c.first_name,
       c.last_name,
       SUM(oi.quantity * p.list_price) over(partition by c.customer_id) as total_revenue,
       MAX(oi.quantity * p.list_price) over(partition by c.customer_id) as max_order,
       MIN(oi.quantity * p.list_price) OVER(partition by c.customer_id) as min_order,
       COUNT(o.order_id) OVER(partition by c.customer_id) as orders_count,
       AVG(oi.quantity * p.list_price) OVER(partition by c.customer_id) as avg_order
from customer c
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id
join product_cor p on oi.product_id = p.product_id
order by total_revenue desc, orders_count DESC;

-- 5 запрос топ3 минимум
select c.customer_id,
    c.first_name,
    c.last_name,
    COALESCE(SUM(oi.quantity * p.list_price), 0) as total_amount
from customer c
left join orders o on c.customer_id = o.customer_id
left join order_items oi on o.order_id = oi.order_id
left join product_cor p on oi.product_id = p.product_id
group by c.customer_id, c.first_name, c.last_name
order by total_amount asc
limit 3

-- 5 запрос топ3 максимум
select c.customer_id,
    c.first_name,
    c.last_name,
    COALESCE(SUM(oi.quantity * p.list_price), 0) as total_amount
FROM customer c
left join orders o on c.customer_id = o.customer_id
left join order_items oi on o.order_id = oi.order_id
left join product_cor p on oi.product_id = p.product_id
group by c.customer_id, c.first_name, c.last_name
order by total_amount desc
limit 3;

-- 6 запрос
select customer_id,
       first_name,
       last_name,
       order_id,
       order_date,
       transaction_amount
from (
    select c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.order_date,
        SUM(oi.quantity * p.list_price) as transaction_amount,
        ROW_NUMBER() OVER(partition by c.customer_id order by o.order_date ASC) as transaction_num
    from customer c
    join orders o on c.customer_id = o.customer_id
    join order_items oi on o.order_id = oi.order_id
    join product_cor p on oi.product_id = p.product_id
    group by c.customer_id, c.first_name, c.last_name, o.order_id, o.order_date
) as numbered_transactions
where transaction_num = 2;


-- 7 запрос
with order_intervals as (
    select c.customer_id,
        c.first_name,
        c.last_name,
        c.job_title,
        o.order_date::DATE as current_order_date,
        LEAD(o.order_date::DATE) OVER(partition by c.customer_id order by o.order_date::DATE) as next_order_date,
        LEAD(o.order_date::DATE) OVER(partition by c.customer_id order by o.order_date::DATE) 
            - o.order_date::DATE as days_between_orders
    from customer c
    join orders o on c.customer_id = o.customer_id
)
select customer_id,
       first_name,
       last_name,
       job_title,
       MAX(days_between_orders) as max_interval_days
from order_intervals
where days_between_orders is not null  -- исключаем последний заказ 
group by customer_id, first_name, last_name, job_title
having COUNT(*) > 1  -- исключаем клиентов, у которрых только один или меньше заказов
order by max_interval_days DESC;


-- 8 запрос
with customer_revenue as (
    select c.first_name,
          c.last_name,
          c.wealth_segment,
          SUM(oi.quantity * p.list_price) as total_revenue
    from customer c
    join orders o on c.customer_id = o.customer_id
    join order_items oi on o.order_id = oi.order_id
    join product_cor p on oi.product_id = p.product_id
    group by c.first_name, c.last_name, c.wealth_segment
),
ranked_customers as (
    select first_name,
        last_name,
        wealth_segment,
        total_revenue,
        ROW_NUMBER() OVER(partition by wealth_segment order by total_revenue desc) as rank_in_segment
    from customer_revenue
)
select first_name,
      last_name,
      wealth_segment,
      total_revenue,
      rank_in_segment
from ranked_customers
where rank_in_segment <= 5
order by wealth_segment, 
         rank_in_segment;

