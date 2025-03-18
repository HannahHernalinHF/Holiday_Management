----- HMT Tracker Dashboard -----

-- To gather the postal codes that have delivery shifts
WITH VIEW_1_HolidayShift AS (
SELECT DISTINCT ed.country_group AS market,
    ed.bob_entity_code AS country,
    business_unit,
    shift_id,
    postal_code,
    origin_option_handle,
    target_option_handle,
    dd.hellofresh_week,
    origin_date,
    target_date,
    visible_to_customer,
    SUBSTRING(fk_imported_at, 1, 8) AS fk_imported_at,
    MIN(REPLACE(SUBSTR(published_time, 1, 10), '-', '')) AS published_date
FROM public_holiday_shift_live.holiday_shift_standardized_ap AS hs
LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS ed
    ON hs.business_unit = ed.bob_entity_code
LEFT JOIN (SELECT DISTINCT hellofresh_week, date_string_backwards FROM dimensions.date_dimension) AS dd
    ON hs.origin_date = dd.date_string_backwards
WHERE meta.operation != 'd'
  AND LEFT(hs.origin_date,4) >= '2024'
  AND hs.business_unit IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
GROUP BY ALL
)



-- To gather the 1-off delivery changes data
, VIEW_2_1OFF AS (
SELECT scs.business_unit
     , scs.fk_subscription AS subscription_id
     , scs.delivery_weekday AS changed_delivery_weekday
     , dd.day_name
     , scs.delivery_time AS changed_delivery_time
     , scs.week_id AS hellofresh_week
     , dd.date_string_backwards AS changed_delivery_date
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
        , ed.bob_entity_code AS country
        , ds.fk_subscription
        , ds.delivery_wk_4 AS delivery_date
        , ds.fk_imported_at_date
        , CASE WHEN ed.bob_entity_code IN ('GB','GN') AND LEN(zip)<=5 THEN SUBSTR(UPPER(zip),1,LEN(zip)-3)
             WHEN ed.bob_entity_code IN ('GB','GN') AND LEN(zip)>5 THEN SUBSTR(UPPER(zip), 1, 4)
             WHEN ed.bob_entity_code IN ('IE') THEN SUBSTR(zip,1,LEN(zip)-4)
             WHEN ed.bob_entity_code IN ('NL','TT','GQ')
                 THEN SUBSTR((REPLACE(UPPER(zip),' ','')),1,LEN(REPLACE(UPPER(zip),' ',''))-2)
             WHEN ed.bob_entity_code IN ('NL') AND zip='NL5042ZK' THEN '5042'
            ELSE UPPER(zip) END AS zip
        --, c.delivery_time AS changed_delivery_time
        , off.changed_delivery_date
--        , ds.delivery_wk_4
        , dd.hellofresh_week
        , COALESCE(off.changed_delivery_time,ds.delivery_time) AS updated_delivery_time
        , COALESCE(off.changed_delivery_date,ds.delivery_wk_4) AS updated_delivery_date
        , off.changed_delivery_weekday
        , ol.tis_is_delivered AS is_delivered
    FROM scm_forecasting_model.delivery_snapshots AS ds
    LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS ed
        ON ds.country = ed.bob_entity_code
    LEFT JOIN (SELECT DISTINCT hellofresh_week, date_string_backwards FROM dimensions.date_dimension) AS dd
        ON ds.delivery_wk_4 = dd.date_string_backwards
    LEFT JOIN VIEW_2_1OFF AS off
        ON ds.country = off.business_unit
        AND ds.fk_subscription = off.subscription_id
        AND dd.hellofresh_week = off.hellofresh_week
    LEFT JOIN (
        SELECT DISTINCT region_code,
                        postal_code,
                        subscription_nk,
                        customer_order_item_nk,
                        order_number,
                        delivery_option_handle,
                        product_handle,
                        option_handle,
                        option_name,
                        hf_week,
                        expected_delivery_date,
                        order_status,
                        tis_box_id,
                        tis_carrier,
                        tis_final_status,
                        tis_final_status_time,
                        tis_is_delivered,
                        tis_is_delivered_on_time,
                        tis_not_on_time_days,
                        tis_not_on_time_hours
        FROM public_ops_dap_bi_staging.order_items_logistics_enriched_ow
        WHERE region_code NOT IN ('CA','US') AND meta.operation != 'd'
          AND is_mealbox=true AND hf_week >= '2024-W01'
        ) AS ol
            ON ds.country = ol.region_code
            AND ds.zip = ol.postal_code
            AND COALESCE(off.changed_delivery_time,ds.delivery_time) = ol.delivery_option_handle
            AND ds.fk_subscription = ol.subscription_nk
            AND dd.hellofresh_week = ol.hf_week
    WHERE ds.delivery_wk_4 >= '2024-01-01'
      AND ds.country IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
      AND ds.delivery_wk_4 IS NOT NULL
)


--- To get the other relevant delivery options data
, VIEW_4_DeliveryOptions AS (
SELECT dd.country_group AS market
    , dd.bob_entity_code AS country
    , option_handle
    , cutoff
    , surcharge_price
    , packing_day
    , delivery_day
    , production_capacity
    , fk_imported_at
FROM logistics_configurator.delivery_option AS do
LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS dd
    ON do.region_code = dd.bob_entity_code
WHERE fk_imported_at>=20240101
  AND region_code IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
ORDER BY 2,8
)


--- To get the postal codes shifts data, add the non-shifted and shifted delivery attributes/options, and add the subscriptions data which is for the comparison of impacted vs non-impacted customers
, VIEW_5_Final AS (
SELECT DISTINCT hs.market
     , hs.country
     , hs.shift_id
     , hs.postal_code
     , hs.visible_to_customer
     , hs.published_date
     --, hs.fk_imported_at
     , LEFT(do1.packing_day,3) AS origin_packing_day
     , do1.cutoff AS origin_cutoff_value
     , LEFT(UPPER(DATE_FORMAT(DATE_SUB(COALESCE(soff.updated_delivery_date,hs.origin_date) , INT(do1.cutoff)),'EEEE')),3) AS origin_cutoff_name
     , do1.surcharge_price AS origin_surcharge_price
     , LEFT(do2.packing_day,3) AS target_packing_day
     , do2.cutoff AS target_cutoff_value
     , LEFT(UPPER(DATE_FORMAT(DATE_SUB(COALESCE(soff.updated_delivery_date,hs.origin_date), INT(do2.cutoff)),'EEEE')),3) AS target_cutoff_name
     , do2.surcharge_price AS target_surcharge_price
     , COALESCE(soff.updated_delivery_time,hs.origin_option_handle) AS updated_origin_time_handle
     , hs.target_option_handle
     , COALESCE(soff.updated_delivery_date,hs.origin_date) AS updated_origin_date
     , COALESCE(hs.target_date,COALESCE(soff.updated_delivery_date,hs.origin_date)) AS updated_target_date
     , dd.hellofresh_week
     , DATEDIFF(COALESCE(hs.target_date,COALESCE(soff.updated_delivery_date,hs.origin_date)),COALESCE(soff.updated_delivery_date,hs.origin_date)) AS day_difference
     , soff.is_delivered
     , COUNT(DISTINCT soff.fk_subscription) AS subscription_count
FROM VIEW_1_HolidayShift AS hs
LEFT JOIN VIEW_4_DeliveryOptions AS do1
    ON hs.country = do1.country
    AND hs.origin_option_handle = do1.option_handle
LEFT JOIN VIEW_4_DeliveryOptions AS do2
    ON hs.country = do2.country
    AND hs.target_option_handle = do2.option_handle
LEFT JOIN VIEW_3_SubscriptionsWith1Off AS soff
    ON hs.country = soff.country
    AND hs.postal_code = soff.zip
    AND hs.origin_option_handle = soff.updated_delivery_time
    AND hs.origin_date = soff.updated_delivery_date
LEFT JOIN dimensions.date_dimension AS dd --- to add the hellofresh_week field
    ON COALESCE(soff.updated_delivery_date,hs.origin_date) = dd.date_string_backwards
WHERE dd.hellofresh_week >= '2024-W01'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)


SELECT *
FROM VIEW_5_Final
