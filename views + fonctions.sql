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