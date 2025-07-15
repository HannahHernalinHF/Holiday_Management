--- Customer Impact Analysis: Delivery Changes Due to Public Holidays (AOR + 1 OFF Changes Incorporated)

-- Get the postal codes for a specific public holiday
WITH VIEW_1_Holiday_Shifts AS (
SELECT DISTINCT business_unit
              , origin_option_handle
              , target_option_handle
              , origin_date
              , target_date
              , postal_code
              , REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
FROM public_holiday_shift_live.holiday_shift_latest
WHERE business_unit IN ('DE')   --- Update preferred BU
    AND origin_date='2025-03-08' --- Update preferred Public Holiday
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
    AND scs.delivery_weekday = (dd.day_of_week + 1)
WHERE scs.business_unit = 'DE'
  AND scs.week_id >= '2024-W10'
  AND scs.delivery_time IS NOT NULL
)


-- Get our base customers/subscriptions that have the public holiday delivery
, VIEW_3_Subscriptions AS (
SELECT *
FROM (
    SELECT sfs.fk_subscription
        , COALESCE(off.changed_delivery_date,sfs.delivery_wk_4) AS delivery_wk_4
        , sfs.fk_imported_at_date
        , sfs.delivery_time
        , CASE WHEN ed.bob_entity_code IN ('GB','GN') AND LEN(sfs.zip)<=5 THEN SUBSTR(UPPER(sfs.zip),1,LEN(sfs.zip)-3)
             WHEN ed.bob_entity_code IN ('GB','GN') AND LEN(sfs.zip)>5 THEN SUBSTR(UPPER(sfs.zip), 1, 4)
             WHEN ed.bob_entity_code IN ('IE') THEN SUBSTR(sfs.zip,1,LEN(sfs.zip)-4)
             WHEN ed.bob_entity_code IN ('NL','TT','GQ')
                 THEN SUBSTR((REPLACE(UPPER(sfs.zip),' ','')),1,LEN(REPLACE(UPPER(sfs.zip),' ',''))-2)
             WHEN ed.bob_entity_code IN ('NL') AND sfs.zip='NL5042ZK' THEN '5042'
            ELSE UPPER(sfs.zip) END AS zip_clean
        , ROW_NUMBER() OVER(PARTITION BY sfs.fk_subscription ORDER BY sfs.fk_imported_at_date) AS rank
    FROM scm_forecasting_model.subscription_forecast_snapshots AS sfs -- scm_forecasting_model.delivery_snapshots AS sfs
    LEFT JOIN (SELECT DISTINCT country_group, country, bob_entity_code FROM dimensions.entity_dimension) AS ed
        ON sfs.country = ed.bob_entity_code
    LEFT JOIN (SELECT DISTINCT hellofresh_week, date_string_backwards FROM dimensions.date_dimension) AS dd
        ON sfs.delivery_wk_4 = dd.date_string_backwards
    LEFT JOIN VIEW_2_1OFF AS off
        ON sfs.country = off.business_unit
        AND sfs.fk_subscription = off.subscription_id
        AND dd.hellofresh_week = off.hellofresh_week
    WHERE sfs.fk_imported_at_date BETWEEN 20250207 AND 20250213       --- Update target date
        AND sfs.country='DE'                                          --- Update country
        AND sfs.delivery_wk_4 BETWEEN '2025-03-08' AND '2025-03-14'   --- Update target date
    GROUP BY 1,2,3,4,5)
WHERE rank=1
)

/*
SELECT DISTINCT delivery_wk_4
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE delivery_wk_4 = '2025-03-08'
  fk_imported_at_date BETWEEN 20240405 AND 20240411
  AND country='BE'
ORDER BY 1
*/


-- Grab the target date from the HMT shift
, VIEW_2B AS (
 SELECT A.*
      , COALESCE(B.target_date,delivery_wk_4) AS target_date
 FROM VIEW_3_Subscriptions AS A
 LEFT JOIN VIEW_1_Holiday_Shifts AS B
    ON a.zip_clean=b.postal_code
    AND a.delivery_time = b.origin_option_handle
)

-- Categorise the customers
, VIEW_2C AS (
SELECT fk_subscription
     , zip_clean
     , delivery_wk_4 AS original_delivery_date
     , target_date AS new_delivery_date
     , (CASE WHEN delivery_wk_4=target_date THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
             WHEN RIGHT(target_date,2)-RIGHT(delivery_wk_4,2)= -1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
             WHEN RIGHT(target_date,2)-RIGHT(delivery_wk_4,2)= 1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
             WHEN RIGHT(target_date,2)-RIGHT(delivery_wk_4,2)= -2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
             WHEN RIGHT(target_date,2)-RIGHT(delivery_wk_4,2)= 2 THEN 'Cohort E (+2)' --- shifted: delivery date is shifted to 2 days later than the public holiday
             WHEN RIGHT(target_date,2)-RIGHT(delivery_wk_4,2)= 3 THEN 'Cohort F (+3)' --- shifted: delivery date is shifted to 3 days later than the public holiday
         END) AS cohort
FROM VIEW_2B
WHERE delivery_wk_4='2025-03-08' -- only focus on PH day for now
)


-- Grab the status for these customers from the first date we used (20240426)
-- First set up the unique list of customers
-- Second, cross join for every day of the month
, VIEW_3A AS (
    SELECT A.fk_subscription
        , B.fk_imported_at_date
    FROM VIEW_2C AS A
    CROSS JOIN (
        SELECT fk_imported_at_date
        FROM scm_forecasting_model.subscription_forecast_snapshots --scm_forecasting_model.delivery_snapshots--
        WHERE country='DE'
          AND fk_imported_at_date BETWEEN 20250207 AND 20250312 -- Update the date 
        GROUP BY 1) AS B
    ORDER BY 1,2
)


-- Third, pull info for the week the PH falls on, moving WOW
, VIEW_3B AS (
    SELECT A.*
        , (CASE
                WHEN DATEDIFF(TO_DATE(CAST(A.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2025-02-07') BETWEEN 0 AND 6 THEN status_wk_4
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2025-02-07') BETWEEN 7 AND 13 THEN status_wk_3
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2025-02-07') BETWEEN 14 AND 20 THEN status_wk_2
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2025-02-07') BETWEEN 21 AND 27 THEN status_wk_1
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2025-02-07') BETWEEN 28 AND 34 THEN status_wk_0
            ELSE 'Other' END) AS status
    FROM VIEW_3A AS A
    LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS B --scm_forecasting_model.delivery_snapshots AS B--
    ON A.fk_subscription=B.fk_subscription
    AND A.fk_imported_at_date=B.fk_imported_at_date
    WHERE B.country='DE'
)


-- Attach the cohort categorisation again
, VIEW_3C AS (
SELECT DISTINCT A.*
              , B.cohort
FROM VIEW_3B AS A
LEFT JOIN VIEW_2C AS B
    ON A.fk_subscription=B.fk_subscription
--WHERE fk_imported_at_date=20240509
)


-- To get the unique customer IDs
, VIEW_5A AS (
SELECT DISTINCT subscription_id
  , customer_uuid
  , create_date
  , ROW_NUMBER() OVER(PARTITION BY subscription_id ORDER BY create_date DESC) AS rank
FROM public_edw_business_mart_live.order_line_items
WHERE bob_entity_code='DE'
  AND hellofresh_delivery_week>='2025-W10' AND hellofresh_delivery_week<='2025-W26'
ORDER BY 1,2,4
)


, VIEW_5B AS (
SELECT DISTINCT subscription_id
     , customer_uuid
FROM VIEW_5A
WHERE rank=1
)


-- Join the tables with the subscriptions having a PH delivery with the customer ID and order item IDs
, VIEW_6 AS (
SELECT DISTINCT
       a.fk_imported_at_date,
       b.customer_uuid,
       a.fk_subscription,
       c.order_line_items_id,
       c.hellofresh_delivery_week
FROM VIEW_3A AS a
LEFT JOIN VIEW_5B AS b
    ON a.fk_subscription = b.subscription_id
LEFT JOIN public_edw_business_mart_live.order_line_items AS c
    ON a.fk_subscription = c.subscription_id
WHERE c.bob_entity_code='DE'
  AND c.hellofresh_delivery_week>='2025-W10' AND c.hellofresh_delivery_week<='2025-W20'
  AND c.order_item_type='Mealboxes'
ORDER BY 1,3,4
)


-- Summarise the data
, FINAL AS (
SELECT cohort AS Cohort
    , status AS Status
    , COUNT(DISTINCT customer_uuid) AS Total_Customers
    , COUNT(DISTINCT CASE WHEN hellofresh_delivery_week BETWEEN '2025-W11' AND '2025-W15' THEN order_line_items_id END) AS Post_5W_BoxCount
    , COUNT(DISTINCT CASE WHEN hellofresh_delivery_week BETWEEN '2025-W11' AND '2025-W20' THEN order_line_items_id END) AS Post_10W_BoxCount
    , ROUND(COUNT(DISTINCT CASE WHEN hellofresh_delivery_week BETWEEN '2025-W11' AND '2025-W15' THEN order_line_items_id END) / COUNT(DISTINCT customer_uuid),2) AS Post_5W_AOR
    , ROUND(COUNT(DISTINCT CASE WHEN hellofresh_delivery_week BETWEEN '2025-W11' AND '2025-W20' THEN order_line_items_id END) / COUNT(DISTINCT customer_uuid),2) AS Post_10W_AOR
FROM VIEW_3C AS a
LEFT JOIN VIEW_6 AS b
    ON a.fk_subscription = b.fk_subscription
GROUP BY 1,2
ORDER BY 1,2
)


SELECT *
FROM FINAL
