#this query is the basis for cohort analysis : purchase per customer, before to be categorized per event, user-type, categoires, time etc ..

WITH order_passed AS
    (SELECT  
        DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)) as date_order, /*because time in PG is ISO 8601*/
        id as order_key,
        status,
        shipping_time,shipping_cost,
        shipping_name,
        company_id,
        `petch-00.public.pg_orders`.by as user_id,
        CAST(order_id as INT64) as order_id

FROM `petch-00.public.pg_orders`
WHERE 
    status NOT LIKE "INCOMPLETED"
    ),

Order_and_user AS
    (SELECT *
    FROM order_passed o
    INNER JOIN (
        SELECT email,
                id as user_id_2,
                username
        FROM `petch-00.public.pg_users_permissions_user` ) 
        u ON u.user_id_2 = o.user_id
        WHERE email NOT LIKE "super@access.local" AND user_id_2  NOT IN (1.0,12497.0, 80.0, 16.0, 33.0, 17.0, 34.0,517.0,8879.0) /* to avoid the admin test, or old data from previsou february that ogged back as march in the sytem*/
    ),

  order_complete as (
      SELECT 
      * 
      FROM Order_and_user o
      LEFT JOIN (
          SELECT
          company_id as merchant_id,
          name as merchant_name ,
          FROM `petch-00.public.pg_companies`) c 
          on c.merchant_id=o.company_id
      ),
 

 repeat_customer as (
     SELECT *,
        CASE WHEN customer_seq >1 THEN 'repeat customer'
            ELSE 'new customer'
            END as repeat_purchase
        FROM (SELECT *,
            DENSE_RANK() over (PARTITION BY cast(user_id as string) ORDER BY date_order) as customer_seq /* forced to use cast os partition works only on str*/
            FROM order_complete)
     
 )
        

SELECT * FROM repeat_customer
ORDER BY date_order desc
