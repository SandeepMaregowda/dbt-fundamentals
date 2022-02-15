with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payment as (

    select * from {{ ref('stg_payments') }}

)

select
o.order_id,
c.customer_id,
sum(amount) as amount
from customers c
inner join orders o
on c.customer_id = o.customer_id
inner join payment p
on p.order_id = o.order_id
where p.status = 'success'
GROUP BY o.order_id,c.customer_id