WITH t_first_purchase AS
    (SELECT  
        date_order,
        DATE_DIFF(date_order,first_purchase_date,MONTH) as month_order,
        FORMAT_DATE('%Y-%m', first_purchase_date) as first_purchase,
        user_id
        FROM (
            SELECT 
            date_order,
            user_id,
            first_value(date_order) over (partition by cast(user_id as string) ORDER BY date_order) AS first_purchase_date
            FROM `petch-00.bi_dashboard.repeat_customer`
                )
    ),
    
/* AGGREGATING PER COHORT : this table will aggregate customer count per first purchase cohort and month order*/
agg_first_purchase as(
    SELECT 
    first_purchase,
    month_order,
    count(distinct(user_id)) as customers
    FROM t_first_purch
    GROUP BY first_purchase, month_order
    )
SELECT * FROM agg_first_purchase  
ORDER BY first_purchase, month_order;
    
