-- 1 запрос
select distinct pc.brand
from product_cor pc
join order_items oi on pc.product_id = oi.product_id 
where pc.standard_cost > 1500
group by pc.brand 
having SUM(oi.quantity) >= 1000;

-- 2 запрос
select order_date,
	   COUNT(order_id) as orders_count,
	   COUNT(distinct customer_id) as customer_count
from orders
where order_date between '2017-04-01' and '2017-04-09'
	  and online_order = TRUE
	  and order_status = 'Approved'
group by order_date;

-- 3 запрос
select distinct job_title
from customer
where job_industry_category = 'IT' 
	  and job_title like 'Senior%'
	  and EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("DOB", 'YYYY-MM-DD'))) > 35  
union all
select distinct job_title
from customer
where job_industry_category = 'Financial Services'
	  and job_title like 'Lead%'
	  and EXTRACT(YEAR FROM AGE(CURRENT_DATE, TO_DATE("DOB", 'YYYY-MM-DD'))) > 35;

-- 4 запрос	  
select distinct pc.brand
from customer c 
join orders o on c.customer_id = o.customer_id 
join order_items oi on o.order_id = oi.order_id 
join product_cor pc on oi.product_id = pc.product_id 
where c.job_industry_category = 'Financial Services'
	 and o.order_status = 'Approved'
except
select distinct pc.brand
from customer c 
join orders o on c.customer_id = o.customer_id 
join order_items oi on o.order_id = oi.order_id 
join product_cor pc on oi.product_id = pc.product_id 
where c.job_industry_category = 'IT'
	  and o.order_status = 'Approved';


-- 5 запрос
select distinct c.customer_id,
	   c.first_name,
	   c.last_name,
	   COUNT(distinct o.order_id) as orders_count
from customer c 
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id 
join product_cor pc  on oi.product_id = pc.product_id 
where o.online_order = true
and pc.brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles')
and c.deceased_indicator = 'N'
and c.property_valuation > (
	select AVG(property_valuation)
	from customer c2 
	where c2.state = c.state
	)
group by c.customer_id, 
		 c.first_name,
		 c.last_name
order by orders_count desc
limit 10;


-- 6 запрос
select distinct c.customer_id,
	   c.first_name,
	   c.last_name
from customer c
where owns_car = 'Yes'
	  and wealth_segment <> 'Mass Customer'
	  and not EXISTS(
		  	select *
			from orders o 
			where o.customer_id = c.customer_id
			  	and o.online_order = true
			  	and o.order_status = 'Approved'
			  	and o.order_date >= '2025-01-01' -- тут я осознанно вывожу за реальный текущий год, т.к. в 
			  	and o.order_date <= '2025-12-31' -- запросе выше возраст 35+ тоже считала от текущей даты
	   );										 -- и понимаю, что в выборку попадёт вся таблица


-- 7 запрос
WITH top5_products as (
	select product_id
	from product_cor
	where product_line = 'Road'
	order by list_price desc
	limit 5
	)
select distinct c.customer_id,
	   c.first_name,
	   c.last_name
from customer c
join orders o on c.customer_id = o.customer_id
join order_items oi on o.order_id = oi.order_id
where c.job_industry_category = 'IT'
	  and o.order_status = 'Approved'
	  and oi.product_id in (select product_id from top5_products)
group by c.customer_id,
		 c.first_name,
	     c.last_name
having count(distinct oi.product_id) = 2;


-- 8 запрос
select c.customer_id,
       c.first_name,
       c.last_name,
       c.job_industry_category
  from customer c
  join orders o on c.customer_id = o.customer_id
  join order_items oi on o.order_id = oi.order_id
  where c.job_industry_category = 'IT'
    and o.order_status = 'Approved'
    and o.order_date between '2017-01-01' and '2017-03-01'
  group by c.customer_id, c.first_name, c.last_name, c.job_industry_category
  having COUNT(distinct o.order_id) >= 3
    and SUM(oi.quantity * oi.item_list_price_at_sale) > 10000
    union
    select c.customer_id,
       c.first_name,
       c.last_name,
       c.job_industry_category
  from customer c
  join orders o on c.customer_id = o.customer_id
  join order_items oi on o.order_id = oi.order_id
  where c.job_industry_category = 'Health'
    and o.order_status = 'Approved'
    and o.order_date between '2017-01-01' and '2017-03-01'
  group by c.customer_id, c.first_name, c.last_name, c.job_industry_category
  having COUNT(distinct o.order_id) >= 3
    and SUM(oi.quantity * oi.item_list_price_at_sale) > 10000;


	   

	  
	 
 