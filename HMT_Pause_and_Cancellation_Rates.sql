--- HMT Analysis: Delivery Changes Due to Public Holidays and its Impact on Customer Behavior (with 1-Off changes incorporate) ---

-- Get the postal codes for a specific public holiday
WITH VIEW_1 AS (
    SELECT DISTINCT business_unit,
        origin_option_handle,
        target_option_handle,
        origin_date,
        target_date,
        postal_code,
        REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
    FROM public_holiday_shift_live.holiday_shift_latest
    WHERE business_unit IN ('GB')
    AND origin_date='2024-08-26' --- Public Holiday
)


-- To get the start date of when HMT was made live for the public holiday above^
, VIEW_1B AS (
SELECT MIN(published_date) AS published_date
FROM VIEW_1
)

-- Get our base customers/subscriptions that have the public holiday delivery
, VIEW_2A AS (
SELECT *
FROM (
    SELECT fk_subscription
        , delivery_wk_4
        , fk_imported_at_date
        , delivery_time
        , SUBSTR(zip,1,LEN(zip)-3) AS zip_clean
        , ROW_NUMBER() OVER(PARTITION BY fk_subscription ORDER BY fk_imported_at_date) AS rank
    FROM scm_forecasting_model.subscription_forecast_snapshots
    WHERE fk_imported_at_date BETWEEN 20240726 AND 20240822
    AND country='GB'
    --AND delivery_wk_4 BETWEEN '2024-04-26' AND '2024-05-27'-- IS NOT NULL // Removed as it cuts most of the delivery week
    AND delivery_wk_4 BETWEEN '2024-08-25' AND '2024-08-31'
    GROUP BY 1,2,3,4,5)
WHERE rank=1
)


/*
SELECT DISTINCT delivery_wk_4
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country='GB'
  --AND delivery_wk_4 = '2024-12-26'
  AND fk_imported_at_date BETWEEN 20241122 AND 20241128
ORDER BY 1
*/

-- Grab the target date from the HMT shift
, VIEW_2B AS (
    SELECT a.*
        , origin_option_handle--COALESCE(origin_option_handle,delivery_time) AS origin_option_handle
        , target_option_handle--COALESCE(target_option_handle,delivery_time) AS target_option_handle
        , b.target_date--COALESCE(b.target_date,delivery_wk_4) AS target_date
        , delivery_wk_4
        , c.hellofresh_week
    FROM VIEW_2A AS a
    LEFT JOIN VIEW_1 AS b
        ON a.zip_clean=b.postal_code
        AND a.delivery_time = b.origin_option_handle
    LEFT JOIN dimensions.date_dimension AS c
        ON COALESCE(B.target_date,delivery_wk_4) = c.date_string_backwards
)

--SELECT delivery_wk_4, target_date, cOUNT(DISTINCT fk_subscription) FROM VIEW_2B GROUP BY 1,2 ORDER BY 1,2  /*

-- To incorporate 1 off changes
, VIEW_6 AS (
SELECT id_subscription_change_schedule AS change_sub_id
     , fk_subscription AS subscription_id
     , business_unit
     , delivery_weekday AS changed_delivery_weekday
     , delivery_time AS changed_delivery_time
     , week_id AS hellofresh_week
     , status
     , REPLACE(SUBSTR(created_at, 1, 10), '-', '') AS created_at --created_at
FROM dl_bob_live_non_pii.subscription_change_schedule
WHERE business_unit = 'GB'
  AND week_id >= '2024-W01'
  AND delivery_time IS NOT NULL
)

, VIEW_7 AS (
SELECT DISTINCT a.fk_subscription,
                a.delivery_wk_4, --COALESCE(DATE_ADD(CAST(delivery_wk_4 AS DATE), CAST(SUBSTRING(b.changed_delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.delivery_wk_4) AS delivery_wk_4,
                a.fk_imported_at_date,
                --a.created_at,
                --b.changed_delivery_time,
                --a.delivery_time,
                --COALESCE(b.changed_delivery_time,a.delivery_time) AS delivery_time,
                a.zip_clean,
                a.origin_option_handle,
                a.target_option_handle,
                COALESCE(a.target_date,COALESCE(DATE_ADD(CAST(delivery_wk_4 AS DATE), CAST(SUBSTRING(b.changed_delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.delivery_wk_4)) AS target_date,
                COALESCE(b.changed_delivery_time,a.delivery_time) AS updated_delivery_time,
                DATEDIFF(COALESCE(DATE_ADD(CAST(delivery_wk_4 AS DATE), CAST(SUBSTRING(b.changed_delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.target_date),
                         COALESCE(DATE_ADD(CAST(delivery_wk_4 AS DATE), CAST(SUBSTRING(b.changed_delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.delivery_wk_4)) AS day_difference,
                COALESCE(DATE_ADD(CAST(delivery_wk_4 AS DATE), CAST(SUBSTRING(b.changed_delivery_time,4,1) - SUBSTRING(a.delivery_time,4,1) AS INT)),a.delivery_wk_4) AS updated_delivery_date
FROM VIEW_2B AS a
LEFT JOIN VIEW_6 AS b
    ON a.fk_subscription = b.subscription_id
    AND a.hellofresh_week = b.hellofresh_week
--WHERE changed_delivery_time IS NOT NULL AND delivery_time!=changed_delivery_time
ORDER BY 1
)


-- Categorise the customers
, VIEW_2C AS (
    SELECT fk_subscription
        , zip_clean
        , updated_delivery_date AS original_delivery_date
        , target_date AS new_delivery_date
        , (CASE WHEN updated_delivery_date=target_date THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
                WHEN DATEDIFF(target_date, updated_delivery_date)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
                WHEN DATEDIFF(target_date, updated_delivery_date)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
                WHEN DATEDIFF(target_date, updated_delivery_date)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
                WHEN DATEDIFF(target_date, updated_delivery_date)=2 THEN 'Cohort E (+2)' --- shifted: delivery date is shifted to 2 days later than the public holiday
                WHEN DATEDIFF(target_date, updated_delivery_date)=-3 THEN 'Cohort F (-3)' --- shifted: delivery date is shifted to 3 days earlier than the public holiday)
                WHEN DATEDIFF(target_date, updated_delivery_date)=3 THEN 'Cohort G (+3)' --- shifted: delivery date is shifted to 3 days later than the public holiday)
                --WHEN DATEDIFF(target_date, updated_delivery_date)=-4 THEN 'Cohort H (-4)' --- shifted: delivery date is shifted to 3 days later than the public holiday)
                --WHEN DATEDIFF(target_date, updated_delivery_date)=4 THEN 'Cohort I (+4)' --- shifted: delivery date is shifted to 3 days later than the public holiday)
            END) AS cohort
    FROM VIEW_7--VIEW_2B --
    WHERE updated_delivery_date='2024-08-26' -- only focus on PH day for now
)



-- Grab the status for these customers from the first date we used (in VIEW_2A CTE)
-- First set up the unique list of customers
-- Second, cross join for every day of the month
, VIEW_3A AS (
    SELECT A.fk_subscription
        , B.fk_imported_at_date
    FROM VIEW_2C AS A
    CROSS JOIN (
        SELECT fk_imported_at_date
        FROM scm_forecasting_model.subscription_forecast_snapshots
        WHERE fk_imported_at_date BETWEEN 20240726 AND 20240831
        GROUP BY 1) AS B
    ORDER BY 1,2
)

-- Third, pull info for the week the PH falls on, moving WOW
, VIEW_3B AS (
    SELECT A.*
        , (CASE WHEN DATEDIFF(TO_DATE(CAST(A.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-07-26') BETWEEN 0 AND 6 THEN status_wk_4
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-07-26') BETWEEN 7 AND 13 THEN status_wk_3
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-07-26') BETWEEN 14 AND 20 THEN status_wk_2
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-07-26') BETWEEN 21 AND 27 THEN status_wk_1
                WHEN DATEDIFF(TO_DATE(CAST(B.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-07-26') BETWEEN 28 AND 34 THEN status_wk_0
            ELSE 'Other' END) AS status
    FROM VIEW_3A AS A
    LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS B
    ON A.fk_subscription=B.fk_subscription
    AND A.fk_imported_at_date=B.fk_imported_at_date
    WHERE B.country='GB'
)


, VIEW_3C AS (
SELECT A.*
     , B.cohort
FROM VIEW_3B AS A
LEFT JOIN VIEW_2C AS B
    ON A.fk_subscription=B.fk_subscription
)


--- To get the daily total subscriptions per cohort
, VIEW_4 AS (
SELECT fk_imported_at_date
    , cohort
    , COUNT(DISTINCT(fk_subscription)) AS Total_Subs
FROM VIEW_3C
GROUP BY 1,2
ORDER BY 2,1
)


-- Summarise the data
, VIEW_5 AS (
SELECT a.fk_imported_at_date AS Date
    , a.status AS Status
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort A (0)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_A
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort B (-1)' THEN a.fk_subscription END) /b.Total_Subs AS Cohort_B
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort C (+1)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_C
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort D (-2)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_D
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort E (+2)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_E
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort F (-3)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_F
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort G (+3)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_G
    , COUNT(DISTINCT CASE WHEN a.cohort = 'Cohort H (+4)' THEN a.fk_subscription END) / b.Total_Subs AS Cohort_H
FROM VIEW_3C AS a
LEFT JOIN VIEW_4 AS b
    ON a.fk_imported_at_date = b.fk_imported_at_date
    AND a.cohort = b.cohort
GROUP BY 1,2,a.cohort,b.Total_Subs
ORDER BY 2,1
)


SELECT Date--DATE_FORMAT(to_date(Date,'yyyyMMdd'),'dd-MMM-yyyy') AS Date
     , Status
     , MAX(Cohort_A) AS Cohort_A
     , MAX(Cohort_B) AS Cohort_B
     , MAX(Cohort_C) AS Cohort_C
     , MAX(Cohort_D) AS Cohort_D
     , MAX(Cohort_E) AS Cohort_E
     , MAX(Cohort_F) AS Cohort_F
     , published_date AS HMT_Date
FROM VIEW_5 AS a
CROSS JOIN VIEW_1B AS b
GROUP BY 1,2,9
ORDER BY 1,2



