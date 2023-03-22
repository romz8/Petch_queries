with sales_data as (
    SELECT
    *,
    sum(weekly_sales) over (partition by sku,month order by week) as running_montly_sales
    FROM(
    SELECT 
    date(date_trunc(order_date,isoweek)) as week,
    date(date_trunc(order_date,month)) as month,
    brand,
    sku,
    sum(qty) as weekly_sales
    FROM petch-01.bi_cleaned.order_product_tags
    group by 1,2,3,4
    order by week desc
    )
    order by week desc
),

average_volume as (
    select
    brand,
    sku,
    max(daily_week) as daily_week,
    max(daily_month) as daily_month
    from (
        select
        *,
        safe_divide(weekly_sales,7) as daily_week,
        safe_divide(running_montly_sales,30) as daily_month
        FROM sales_data
        where month = date(date_trunc(current_date('CET'),month)) or month = date_sub(date(date_trunc(current_date('CET'),month)),INTERVAL 1 MONTH)
    )
    group by 1,2
),

join_stocks as (
    SELECT
    *
    FROM `petch-01.bi_cleaned.stock_products` i
    left join (select 
                *,
                case when daily_week is null then 0.11 
                    when daily_week > daily_month then daily_week 
                    else daily_month end as daily_safe 
                from average_volume) v on i.sku = v.sku
    )

SELECT
*,
round(safe_divide(inventory_quantity,daily_safe),0) as stock_runaway
FROM join_stocks
