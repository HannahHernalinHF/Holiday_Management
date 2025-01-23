----- HMT Tracker Dashboard -----

-- To gather the postal codes that have delivery shifts
WITH VIEW_1 AS (
SELECT DISTINCT
    CASE WHEN business_unit IN ('AO','AU','YE','NZ') THEN 'ANZ'
        WHEN business_unit IN ('BE','TO','NL','LU','GQ','TT') THEN 'BNL'
        WHEN business_unit IN ('AT','CH','DE') THEN 'DACH'
        WHEN business_unit IN ('DK','NO','SE') THEN 'NORDICS'
        WHEN business_unit IN ('GB','GN') THEN 'GB'
        ELSE business_unit END AS market,
    CASE WHEN business_unit IN ('AO','AU','YE') THEN 'AU'
        WHEN business_unit IN ('BE','TO') THEN 'BE'
        WHEN business_unit IN ('NL','GQ','TT') THEN 'NL'
        ELSE business_unit END AS country,
    business_unit,
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
FROM public_holiday_shift_live.holiday_shift_standardized_ap
WHERE meta.operation != 'd'
  AND year>=2024
  AND business_unit IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')--('AO','AU','YE','BE','TO','NL','LU','GQ','TT','AT','CH','DE','DK','NO','SE','GB','GN','ES','FR','IE','IT','NZ')
)


-- To gather customers/subscriptions data and incorporate with 1-off delivery changes
, VIEW_2 AS (
    SELECT DISTINCT
        CASE WHEN a.country IN ('AO','AU','YE','NZ') THEN 'ANZ'
            WHEN a.country IN ('BE','TO','NL','LU','GQ','TT') THEN 'BNL'
            WHEN a.country IN ('AT','CH','DE') THEN 'DACH'
            WHEN a.country IN ('DK','NO','SE') THEN 'NORDICS'
            WHEN a.country IN ('GB','GN') THEN 'GB'
            ELSE a.country END AS market
        , CASE WHEN a.country IN ('AO','AU','YE') THEN 'AU'
            WHEN a.country IN ('BE','TO') THEN 'BE'
            WHEN a.country IN ('NL','GQ','TT') THEN 'NL'
            ELSE a.country END AS country
        , a.fk_subscription
        , a.delivery_wk_4 AS delivery_date
        --, b.hellofresh_week
        , a.fk_imported_at_date
        --, a.delivery_time
        , CASE WHEN a.country IN ('GB','IE') THEN SUBSTR(zip,1,LEN(zip)-3) ELSE zip END AS zip
        , c.delivery_weekday AS changed_delivery_weekday
        --, c.delivery_time AS changed_delivery_time
        , COALESCE(c.delivery_time,a.delivery_time) AS updated_delivery_time
        , SUBSTRING(c.delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS day_difference
        , COALESCE(DATE_ADD(CAST(a.delivery_wk_4 AS DATE), CAST(SUBSTRING(c.delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.delivery_wk_4) AS updated_delivery_date
        --, c.status
    FROM scm_forecasting_model.delivery_snapshots AS a
    LEFT JOIN (SELECT DISTINCT hellofresh_week, date_string_backwards FROM dimensions.date_dimension) AS b
        ON a.delivery_wk_4 = b.date_string_backwards
    LEFT JOIN (SELECT *,
                   CASE WHEN business_unit IN ('AO','AU','YE') THEN 'AU'
                       WHEN business_unit IN ('BE','TO') THEN 'BE'
                       WHEN business_unit IN ('NL','GQ','TT') THEN 'NL'
                       ELSE business_unit END AS country
               FROM dl_bob_live_non_pii.subscription_change_schedule
               WHERE business_unit IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
                 AND week_id >= '2024-W01'
                 AND delivery_time IS NOT NULL
               ) AS c
        ON a.country = c.country
        AND a.fk_subscription = c.fk_subscription
        AND b.hellofresh_week = c.week_id
    WHERE a.fk_imported_at_date>=20240101
      AND a.country IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
      AND a.delivery_wk_4 IS NOT NULL
      --AND a.delivery_time IS NOT NULL AND c.delivery_time IS NOT NULL
)


--- To get the other relevant delivery options data
, VIEW_3 AS (
SELECT
    CASE WHEN region_code IN ('AO','AU','YE','NZ') THEN 'ANZ'
        WHEN region_code IN ('BE','TO','NL','LU','GQ','TT') THEN 'BNL'
        WHEN region_code IN ('AT','CH','DE') THEN 'DACH'
        WHEN region_code IN ('DK','NO','SE') THEN 'NORDICS'
        WHEN region_code IN ('GB','GN') THEN 'GB'
        ELSE region_code END AS market,
    CASE WHEN region_code IN ('AO','AU','YE') THEN 'AU'
        WHEN region_code IN ('BE','TO') THEN 'BE'
        WHEN region_code IN ('NL','GQ','TT') THEN 'NL'
        ELSE region_code END AS country
    , option_handle
    , cutoff
    , surcharge_price
    , packing_day
    , delivery_day
    , production_capacity
    , fk_imported_at
FROM logistics_configurator.delivery_option_latest
WHERE fk_imported_at>=20240101
  AND region_code IN ('AU','BE','NL','LU','AT','CH','DE','DK','NO','SE','GB','ES','FR','IE','IT','NZ')
ORDER BY 2,8
)


--- To get the postal codes shifts data, join with the shifted/non-shifted delivery attributes, and the subscriptions data which is for impacted and non-impacted comparison
, VIEW_4 AS (
SELECT a.*
     , b.packing_day
     , b.cutoff
     , b.surcharge_price
     , c.packing_day AS packing_day_shift
     , c.cutoff AS cutoff_shift
     , c.surcharge_price AS surcharge_price_shift
     , d.fk_subscription
     , d.updated_delivery_time
     , d.updated_delivery_date
     , e.hellofresh_week
     , d.day_difference
FROM VIEW_1 AS a
LEFT JOIN VIEW_3 AS b
    ON a.country=b.country
    AND a.origin_option_handle=b.option_handle
LEFT JOIN VIEW_3 AS c
    ON a.country=b.country
    AND a.target_option_handle = c.option_handle
LEFT JOIN VIEW_2 AS d
    ON a.country = d.country
    AND a.postal_code = d.zip
    AND a.origin_option_handle = d.updated_delivery_time
LEFT JOIN dimensions.date_dimension AS e --- to add the hellofresh_week field
        ON d.updated_delivery_date = e.date_string_backwards
)


SELECT *
FROM VIEW_4
