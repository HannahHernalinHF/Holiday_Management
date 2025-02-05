----- HMT Tracker Dashboard -----

-- To gather the postal codes that have delivery shifts
WITH VIEW_1_HolidayShift AS (
SELECT DISTINCT ed.country_group AS market,
    ed.country,
    business_unit,
    shift_id,
    postal_code,
    origin_option_handle,
    target_option_handle,
    origin_date,
    target_date,
    visible_to_customer,
    REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date,
    year,
    month,
    day,
    SUBSTRING(fk_imported_at, 1, 8) AS fk_imported_at
FROM public_holiday_shift_live.holiday_shift_standardized_ap AS hs
LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS ed
    ON hs.business_unit = ed.bob_entity_code
WHERE meta.operation != 'd'
  AND year>=2024
  AND business_unit IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
)


-- To gather the 1-off delivery changes data
, VIEW_2_1OFF AS (
SELECT scs.business_unit
     , scs.fk_subscription AS subscription_id
     , scs.delivery_weekday AS changed_delivery_weekday
     , dd.day_name
     , scs.delivery_time AS changed_delivery_time
     , scs.week_id AS hellofresh_week
     --, scs.status
     , dd.date_string_backwards AS changed_delivery_date
     --, REPLACE(SUBSTR(scs.created_at, 1, 10), '-', '') AS created_at --created_at
FROM dl_bob_live_non_pii.subscription_change_schedule AS scs
LEFT JOIN dimensions.date_dimension AS dd
    ON scs.week_id = dd.hellofresh_week
    AND scs.delivery_weekday = (dd.day_of_week + 1)  -- In date dimensions table, Monday is 0. In SCS table, Monday is 1, so we have do to +1
WHERE scs.business_unit IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
  AND scs.week_id >= '2024-W01'
  AND scs.delivery_time IS NOT NULL
)


-- To gather customers/subscriptions data
, VIEW_3_SubscriptionsWith1Off AS (
    SELECT DISTINCT
          ed.country_group AS market
        , ed.country AS country
        , ds.fk_subscription
        , ds.delivery_wk_4 AS delivery_date
        , ds.fk_imported_at_date
        , CASE WHEN ed.country IN ('GB','GN') THEN SUBSTR(zip,1,LEN(zip)-3)
             WHEN ed.country IN ('IE') THEN SUBSTR(zip,1,LEN(zip)-4)
             WHEN ed.country IN ('NL','TT','GQ') AND REPLACE(UPPER(zip), ' ','')=6
                 THEN SUBSTR(zip,1,LEN(zip)-2)
             WHEN ed.country IN ('NL') AND zip='NL5042ZK'
                 THEN LEFT('5042ZK',4)
            ELSE zip END AS zip
        --, c.delivery_time AS changed_delivery_time
        , off.changed_delivery_date
        , ds.delivery_wk_4
        , COALESCE(off.changed_delivery_time,ds.delivery_time) AS updated_delivery_time
        , COALESCE(off.changed_delivery_date,ds.delivery_wk_4) AS updated_delivery_date
        , off.changed_delivery_weekday
    FROM scm_forecasting_model.delivery_snapshots AS ds
    LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS ed
        ON ds.country = ed.bob_entity_code
    LEFT JOIN (SELECT DISTINCT hellofresh_week, date_string_backwards FROM dimensions.date_dimension) AS dd
        ON ds.delivery_wk_4 = dd.date_string_backwards
    LEFT JOIN VIEW_2_1OFF AS off
        ON ds.country = off.business_unit
        AND ds.fk_subscription = off.subscription_id
        AND dd.hellofresh_week = off.hellofresh_week
    WHERE ds.fk_imported_at_date>=20240101 AND ds.delivery_wk_4>='2021-01-01'
      AND ds.country IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
      AND ds.delivery_wk_4 IS NOT NULL
)


--- To get the other relevant delivery options data
, VIEW_3_DeliveryOptions AS (
SELECT b.country_group AS market
    , b.country
    , option_handle
    , cutoff
    , surcharge_price
    , packing_day
    , delivery_day
    , production_capacity
    , fk_imported_at
FROM logistics_configurator.delivery_option AS a
LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS b
    ON a.region_code = b.bob_entity_code
WHERE fk_imported_at>=20240101
  AND region_code IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
ORDER BY 2,8
)


--- To get the postal codes shifts data, add the non-shifted and shifted delivery attributes/options, and add the subscriptions data which is for the comparison of impacted vs non-impacted customers
, VIEW_4_Final AS (
SELECT hs.market
     , hs.country
     , hs.shift_id
     , hs.postal_code
     , hs.visible_to_customer
     , hs.published_date
     , hs.year
     , hs.month
     , hs.day
     , hs.fk_imported_at
     , do1.packing_day AS origin_packing_day
     , do1.cutoff AS origin_cutoff
     , do1.surcharge_price AS origin_surcharge_price
     , do2.packing_day AS target_packing_day
     , do2.cutoff AS target_cutoff
     , do2.surcharge_price AS target_surcharge_price
     , COALESCE(soff.updated_delivery_time,hs.origin_option_handle) AS updated_origin_time_handle
     , hs.target_option_handle
     , COALESCE(soff.updated_delivery_date,hs.origin_date) AS updated_origin_date
     , hs.target_date
     , dd.hellofresh_week
     , DATEDIFF(hs.target_date,COALESCE(soff.updated_delivery_date,hs.origin_date)) AS day_difference
     , COUNT(DISTINCT soff.fk_subscription) AS subscription_count
FROM VIEW_1_HolidayShift AS hs
LEFT JOIN VIEW_3_DeliveryOptions AS do1
    ON hs.country = do1.country
    AND hs.origin_option_handle = do1.option_handle
LEFT JOIN VIEW_3_DeliveryOptions AS do2
    ON hs.country = do2.country
    AND hs.target_option_handle = do2.option_handle
LEFT JOIN VIEW_3_SubscriptionsWith1Off AS soff
    ON hs.country = soff.country
    AND hs.postal_code = soff.zip
    AND hs.origin_option_handle = soff.updated_delivery_time
    AND hs.origin_date = soff.updated_delivery_date
LEFT JOIN dimensions.date_dimension AS dd --- to add the hellofresh_week field
        ON soff.updated_delivery_date = dd.date_string_backwards
WHERE LENGTH(soff.zip)>2
GROUP BY ALL
)


SELECT *
FROM VIEW_4_Final
