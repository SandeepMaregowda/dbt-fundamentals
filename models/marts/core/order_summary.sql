{%- set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] -%}
with payments as
(
    select * from {{ ref('stg_payments') }}
),
final as
(
    select
        order_id,
        {% for payment_method in payment_methods %}
            sum(case when paymentmethod = '{{payment_method}}' then amount else 0 end) as {{payment_method}}_amount
            {%- if not loop.last -%}
            ,
            {%- endif -%}
        {% endfor %}
    from
        payments
    where
        status = 'success'
    group by 1
)
Select * from final