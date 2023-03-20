#this query is the basis for cohort analysis : purchase per customer, before to be categorized per event, user-type, categoires, time etc ..

WITH order_passed AS
    (SELECT  
        PARSE_DATE("%F",regexp_extract(created_at,r"\d{4}-\d{2}-\d{2}")) as date_order, /*because time in PG is ISO 8601*/
        id as order_key,
        status,
        shipping_time,shipping_cost,
        shipping_name,
        company_id,
        `petch-00.public.pg_orders`.by as user_id,
        CAST(order_id as INT64) as order_id

FROM `petch-00.public.pg_orders`
WHERE 
    status NOT LIKE "INCOMPLETED" AND status NOT LIKE "REFUSED"
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
        WHERE email NOT LIKE "super@access.local" AND user_id_2  NOT IN (1.0,12497.0, 134.0, 80.0, 16.0, 33.0, 17.0, 34.0,517.0)/* to avoid the admin test, or old data from previsou february that ogged back as march in the sytem*/
    ),

order_component AS (
    SELECT * 
    FROM order_and_user o
    LEFT JOIN (
        SELECT id,
                field, 
                order_id as order_key_2, 
                component_id, 
                component_type 
        FROM `petch-00.public.pg_orders_components`) c on c.order_key_2=o.order_key
),
order_products as(
    SELECT * 
    FROM order_component c
    LEFT JOIN (
        SELECT
        id as component_ref,
        sku,
        gtin,
        name,
        regexp_extract(listing_id,r"[0-9]+") as listing_id
        FROM `petch-00.public.pg_product`) 
        p on p.component_ref=c.component_id
),
order_prod_address_shipping as (
    SELECT * 
    FROM order_products p
    INNER JOIN (
        SELECT 
        id as component_address,
        city,
        company as b2b_user_company,
        country,
        zipcode,
        phone_number
        FROM `petch-00.public.pg_components_order_addresses`) a 
        on a.component_address=p.component_id
        LEFT JOIN (
            SELECT 
            id shipment_id,
            type,
            price, 
            company
            FROM `petch-00.public.pg_components_order_shipments`) as s
            on s.shipment_id=p.component_id

        ),
  order_complete as (
      SELECT 
        date_order,
    status ,
    company_id,
    merchant_name,
    merchant_country,
    order_id,
    email,
    username,
    user_id,
    (price - shipping_cost) as order_net_shipping,
    shipping_cost,
    price as total_basket,
    ROUND(100*(shipping_cost)/(price - shipping_cost),2) as shipping_as_percentage_total,
    shipping_time,
    shipping_name,
    city,
    b2b_user_company,
    country as user_country,
    zipcode
      FROM order_prod_address_shipping o
      INNER JOIN (
          SELECT
          company_id as merchant_id,
          name as merchant_name ,
          country as merchant_country
          FROM `petch-00.public.pg_companies`) c 
          on c.merchant_id=o.company_id
      ),
 grouped_orders as(/*forced to group orders otherwise the totla count of orders (when calculating repeat or new customer) will be wrong*/
    SELECT 
        date_order,
        status,
        merchant_name,
        company_id,
        order_id,
        user_id,
        username,
        email,
        city,
        zipcode,
    b2b_user_company,
    FROM order_complete
    Group by order_id,
    date_order,merchant_name,
        company_id,
        status,
        order_id,
        user_id,
        username,
        email,
        city,
        zipcode,
    b2b_user_company
        ),

B2B_consumer as (
    SELECT*,
    case when b2b_user_company not like ""
    then 1 else 0 end as B2B_customer
    FROM grouped_orders
),

 repeat_customer as (
     SELECT *,
        CASE WHEN customer_seq >1 THEN 'repeat customer'
            ELSE 'new customer'
            END as repeat_purchase
        FROM (SELECT *,
            RANK() over (PARTITION BY cast(user_id as string) ORDER BY date_order) as customer_seq /* forced to use cast os partition works only on str*/
            FROM B2B_consumer )
     
 )

SELECT * FROM repeat_customer
Order by date_order
