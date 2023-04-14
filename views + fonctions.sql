CREATE OR REPLACE FUNCTION distance(
    lat1 double precision,
    lon1 double precision,
    lat2 double precision,
    lon2 double precision)
  RETURNS double precision AS
$BODY$
DECLARE
    R integer = 6371e3; -- Meters
    rad double precision = 0.01745329252;
    φ1 double precision = lat1 * rad;
    φ2 double precision = lat2 * rad;
    Δφ double precision = (lat2-lat1) * rad;
    Δλ double precision = (lon2-lon1) * rad;
    a double precision = sin(Δφ/2) * sin(Δφ/2) + cos(φ1) * cos(φ2) * sin(Δλ/2) * sin(Δλ/2);
    c double precision = 2 * atan2(sqrt(a), sqrt(1-a));    
BEGIN                                                     
    RETURN R * c;        
END  
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

create or replace view items_info as
  select
    oi.*,
    p.product_category_name,
    s.seller_zip_code_prefix,
    gs.geolocation_lat  as seller_lat,
    gs.geolocation_lng  as seller_lng,
    c.customer_zip_code_prefix,
    gc.geolocation_lat  as customer_lat,
    gc.geolocation_lng  as customer_lng,
    distance(gs.geolocation_lat, gs.geolocation_lng, gc.geolocation_lat, gc.geolocation_lng) / 1000 as distance,
    avg(r.review_score) as score_moy
  from order_items oi
  left join products p
    using (product_id)
  left join orders o
    using (order_id)
  left join customers c
    using (customer_id)
  left join sellers s
    using (seller_id)
  left join order_reviews r
    using (order_id)
  left join geolocation gc
    on c.customer_zip_code_prefix = gc.geolocation_zip_code_prefix
  left join geolocation gs
    on s.seller_zip_code_prefix = gs.geolocation_zip_code_prefix
  group by
    oi.order_id,
    oi.order_item_id,
    p.product_id,
    s.seller_id,
    c.customer_id,
    gs.geolocation_zip_code_prefix,
    gc.geolocation_zip_code_prefix
;

create view order_info as
	select
		order_id,
		nb_items,
		nb_payments,
		price
	from (
	    select
	    	order_id,
	    	count(*) as nb_items,
	    	sum(price) as price
	   	from order_items oi
	   	group by order_id
	    ) as a
	join (
	    select
	    	order_id,
	    	count(*) as nb_payments
	    from order_payments op
	    group by order_id
	    ) as b
	  	using (order_id)
;

create or replace view Customer_unique_spending as 
	select customer_unique_id, count(customer_id) as nb_order, sum(nb_product) as total_product, sum(sum_price) as total_sum  from (
		select order_id, customer_id, count(product_id) as nb_product, sum(price) as sum_price from orders
		left join order_items oi
		using (order_id)
		group by order_id, customer_id
	) as a
	left join (
		select customer_unique_id, customer_id from customers
		) as b
		using (customer_id)
	group by customer_unique_id
	;

create materialized view customer_first_order as
	select 
		customer_unique_id,
		nb_order,
		nb_order_approved,
		first_order_id,
		first_order_timestamp
	from (
		select
			c.customer_unique_id,
			count(*) as nb_order
		from customers c
		join orders o
			using (customer_id)
		group by c.customer_unique_id
	) t
	left join (
		select
			c.customer_unique_id,
			count(*) as nb_order_approved
		from customers c
		join orders o
			using (customer_id)
		where o.order_approved_at is not null
		group by c.customer_unique_id
	) as t2
		using (customer_unique_id)
	left join (
		select
		    c.customer_unique_id,
		    o.order_id as first_order_id,
		    t.first_order_timestamp
		from customers c
		left join orders o 
		    using (customer_id)
		inner join (
		    select
		        c2.customer_unique_id,
		        min(o2.order_approved_at) as first_order_timestamp
		    from customers c2
		    left join orders o2
		        using (customer_id)
		    group by c2.customer_unique_id
		) t
		on (
		    c.customer_unique_id = t.customer_unique_id and
		    o.order_approved_at = t.first_order_timestamp
		)
	) as t3
		using (customer_unique_id)
;

create view time_order as select
    t.order_id,
    g.geolocation_state,
    t.order_purchase_timestamp,
    t.approvement_time,
    t.carrier_deliver_time,
    t.customer_deliver_time,
    t.first_review_after_delivery,
    t.gap_estimated_delivery
from (
    select
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_approved_at - o.order_purchase_timestamp as "approvement_time",
        o.order_delivered_carrier_date - o.order_purchase_timestamp as "carrier_deliver_time",
        o.order_delivered_customer_date - o.order_purchase_timestamp as "customer_deliver_time",
        min(or2.review_creation_date) - o.order_delivered_customer_date as "first_review_after_delivery",
        o.order_delivered_customer_date - o.order_estimated_delivery_date as "gap_estimated_delivery"
    from orders o
    left join order_reviews or2
        using (order_id)
    group by order_id
    ) as t
left join customers c
    using (customer_id)
left join geolocation g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix 
;

/* vues multiples question 1 */
drop view if exists question1_join cascade;

create view question1_join as 
  SELECT 
    oi."order_id" AS "order_id", 
    oi."order_item_id" AS "order_item_id", 
    oi."product_id" AS "product_id", 
    p.product_category_name as product_category_name,
    oi."seller_id" AS "seller_id", 
    oi."shipping_limit_date" AS "shipping_limit_date", 
    oi."price" AS "price", 
    oi."freight_value" AS "freight_value", 
    o."customer_id" AS "customer_id", 
    o."order_status" AS "order_status", 
    o."order_purchase_timestamp" AS "order_purchase_timestamp", 
    c."customer_zip_code_prefix" AS "customer_zip_code_prefix", 
    bs."abbreviation" AS "abbreviation", 
    bs."region" AS "region",
    DATE(date_trunc('month', order_purchase_timestamp) + interval '1 month - 1 day') as date_truncated
  FROM order_items oi 
  LEFT JOIN orders AS o ON oi."order_id" = o."order_id" 
  LEFT JOIN customers AS c ON o."customer_id" = c."customer_id"
  left join geolocation as g on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix 
  LEFT JOIN brazil_states AS bs ON g.geolocation_state  = bs."abbreviation"
  left join products p on oi.product_id = p.product_id
  where order_status != 'canceled'


/* creer vues par region avec les 10 produits au plus gros CA */
drop view if exists question1_Center_West;
create view question1_Center_West as 
  select 
    max(product_id) as product_id , 
    max(region) as region, 
    count(product_id) as volume,
    count(distinct order_id) as commandes,
    sum(price) as chiffre_aff
  FROM  question1_join q 
  WHERE region = 'Center West'
  GROUP BY q.product_id
  ORDER BY chiffre_aff desc
  limit 10;

drop view if exists question1_North;
create view question1_North as 
  select 
    max(product_id) as product_id , 
    max(region) as region, 
    count(product_id) as volume,
    count(distinct order_id) as commandes,
    sum(price) as chiffre_aff
  FROM question1_join q 
  WHERE region = 'North'
  GROUP BY q.product_id
  ORDER BY chiffre_aff desc
  limit 10;

drop view if exists question1_Northeast;
create view question1_Northeast as 
  select 
    max(product_id) as product_id , 
    max(region) as region, 
    count(product_id) as volume,
    count(distinct order_id) as commandes,
    sum(price) as chiffre_aff
  FROM  question1_join q 
  WHERE region = 'Northeast'
  GROUP BY q.product_id
  ORDER BY chiffre_aff desc
  limit 10;

drop view if exists question1_Southeast;
create view question1_Southeast as 
  select 
    max(product_id) as product_id , 
    max(region) as region, 
    count(product_id) as volume,
    count(distinct order_id) as commandes,
    sum(price) as chiffre_aff
  FROM 
  question1_join q 
  WHERE region = 'Southeast'
  GROUP BY q.product_id
  ORDER BY chiffre_aff desc
  limit 10;

drop view if exists question1_South;
create view question1_South as 
  select 
    max(product_id) as product_id , 
    max(region) as region, 
    count(product_id) as volume,
    count(distinct order_id) as commandes,
    sum(price) as chiffre_aff
  FROM 
  question1_join q 
  WHERE region = 'South'
  GROUP BY q.product_id
  ORDER BY chiffre_aff desc
  limit 10;

/* creation view pour question 6*/
drop view question6_join;

create view question6_join as 
SELECT oi."order_id" AS "order_id", 
oi."order_item_id" AS "order_item_id", 
oi."product_id" AS "product_id", 
p.product_category_name as product_category_name,
oi."seller_id" AS "seller_id", 
oi."price" AS "price", 
oi."freight_value" AS "freight_value", 
o."customer_id" AS "customer_id", 
o."order_status" AS "order_status", 
bsc."abbreviation" AS customer_state, 
bsc."region" AS "customer_region",
bss."abbreviation" AS seller_state, 
bss."region" AS "seller_region",
DATE(date_trunc('month', order_purchase_timestamp) + interval '1 month - 1 day') as date_truncated,
CASE 
     WHEN bsc."region"=bss."region" THEN 'intra'
     ELSE 'inter'
END as intra_inter_region
FROM order_items oi 
LEFT JOIN orders o ON oi."order_id" = o."order_id" 
LEFT JOIN customers c ON o."customer_id" = c."customer_id"
left join geolocation gc on c.customer_zip_code_prefix = gc.geolocation_zip_code_prefix 
left join sellers s on oi.seller_id = s.seller_id
left join geolocation gs on s.seller_zip_code_prefix = gs.geolocation_zip_code_prefix 
LEFT JOIN brazil_states bsc ON gc.geolocation_state  = bsc."abbreviation"
LEFT JOIN brazil_states bss ON gs.geolocation_state  = bss."abbreviation"
left join products p on oi.product_id = p.product_id
where order_status !='canceled'
;

create or replace view quality as
    select product_id, product_description_lenght, product_photos_qty, count(order_id) as nb_order from products p
    left join order_items oi 
        using (product_id)
    group by product_id, product_name_lenght, product_photos_qty
    order by nb_order desc
;

CREATE ROLE orm_user
WITH LOGIN
PASSWORD 'api_orm';

GRANT select, insert, update, delete ON product_category_name_translation TO orm_user;