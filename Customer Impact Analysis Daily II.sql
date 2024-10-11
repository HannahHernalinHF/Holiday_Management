--- Customer Impact Analysis: Delivery Changes Due to Public Holidays (Daily)

WITH VIEW_1 AS ( --- to get the postal codes for a specific public holiday
SELECT DISTINCT business_unit,
       origin_option_handle,
       target_option_handle,
       origin_date,
       target_date,
       postal_code,
       REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
FROM public_holiday_shift_live.holiday_shift_latest
WHERE business_unit IN ('GB')
  AND origin_date='2024-05-27' --- UK Public Holiday
)

, VIEW_1B AS ( --- to get the start date of when HMT was made live
SELECT min(published_date) AS min_published_date
FROM VIEW_1
)

, VIEW_2A AS (  --- to get our base customers
SELECT fk_subscription
     , delivery_wk_4
     , fk_imported_at_date
     , delivery_time
     , SUBSTR(zip,1,LEN(zip)-3) AS zip_clean
     , ROW_NUMBER() OVER(PARTITION BY fk_subscription ORDER BY fk_imported_at_date DESC) AS rank
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE fk_imported_at_date BETWEEN 20240426 AND 20240527
  AND country='GB'
  AND delivery_wk_4 IS NOT NULL
  GROUP BY 1,2,3,4,5
)

, VIEW_2B AS (
SELECT DISTINCT fk_subscription
              , delivery_time
FROM VIEW_2A
WHERE rank=1
)

, VIEW_2C AS (
SELECT a.fk_subscription
       , a.delivery_time
       , '2024-05-27' AS delivery_date --b.delivery_wk_4
       , CASE WHEN b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END AS status--status_wk_4
       /*, (CASE WHEN b.delivery_wk_1='2024-05-27' THEN b.delivery_wk_1
           WHEN b.delivery_wk_2='2024-05-27' THEN b.delivery_wk_2
           WHEN b.delivery_wk_3='2024-05-27' THEN b.delivery_wk_3
           WHEN b.delivery_wk_4='2024-05-27' THEN b.delivery_wk_4
           ELSE NULL END) AS delivery_date
       , (CASE
           WHEN b.delivery_wk_1='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_1 END)
           WHEN b.delivery_wk_2='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_2 END)
           WHEN b.delivery_wk_3='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_3 END)
           WHEN b.delivery_wk_4='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END)
           ELSE NULL END) AS status */
       , SUBSTR(b.zip,1,LEN(b.zip)-3) AS zip
       , b.fk_imported_at_date
       , c.min_published_date AS published_date
FROM VIEW_2B AS a
LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.delivery_time = b.delivery_time
CROSS JOIN VIEW_1B AS c
--WHERE b.delivery_wk_4 IS NULL OR b.delivery_wk_4='2024-05-27'
--WHERE b.fk_imported_at_date BETWEEN 20240426 AND 20240527  --b.fk_imported_at_date>=20240426 AND b.fk_imported_at_date<=20240527 --AND b.delivery_wk_4>='2024-01-01'
ORDER BY 1,6--8
)

, VIEW_3 AS ( --- to group into different cohorts
SELECT a.fk_subscription
     , target_date
     , delivery_date
     , CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
     , status--CASE WHEN status_wk_4='cancelled' THEN 'cancelled' ELSE status END AS status --status_wk_4 AS status--
     , a.fk_imported_at_date
     , a.published_date
FROM VIEW_2C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip=b.postal_code
    AND a.delivery_time = b.origin_option_handle
WHERE a.fk_imported_at_date BETWEEN 20240426 AND 20240527
ORDER BY 1,6
)


, VIEW_3B AS (
SELECT fk_subscription
     , MAX(CASE WHEN fk_imported_at_date<published_date THEN fk_imported_at_date END) AS prehmt
     , MIN(CASE WHEN fk_imported_at_date>=published_date THEN fk_imported_at_date END) AS hmt
FROM VIEW_3
GROUP BY 1
HAVING MIN(CASE WHEN fk_imported_at_date>=published_date THEN fk_imported_at_date END) IS NOT NULL
    AND MAX(CASE WHEN fk_imported_at_date<published_date THEN fk_imported_at_date END) IS NOT NULL
ORDER BY 1
)

, VIEW_4 AS (
SELECT DISTINCT a.fk_subscription
     , b.cohort
     , b.status
     , b.fk_imported_at_date
     , b.delivery_date
     , b.published_date
FROM VIEW_3B AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_subscription=b.fk_subscription
WHERE status IS NOT NULL
ORDER BY 1,4
)

SELECT cohort,
       status,
       delivery_date,
       fk_imported_at_date,
       count(DISTINCT fk_subscription)
FROM VIEW_4
WHERE fk_imported_at_date BETWEEN 20240426 AND 20240527
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4


/*

WITH VIEW_1 AS ( --- to get the postal codes for a specific public holiday
SELECT DISTINCT business_unit,
       origin_option_handle,
       target_option_handle,
       origin_date,
       target_date,
       postal_code,
       REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
FROM public_holiday_shift_live.holiday_shift_latest
WHERE business_unit IN ('GB')
  AND origin_date='2024-05-27' --- UK Public Holiday
)

, VIEW_1B AS ( --- to get the start date of when HMT was made live
SELECT min(published_date) AS min_published_date
FROM VIEW_1
)

, VIEW_2A AS (
SELECT fk_subscription
     , delivery_wk_4
     , fk_imported_at_date
     , delivery_time
     , ROW_NUMBER() OVER(PARTITION BY fk_subscription ORDER BY fk_imported_at_date DESC) AS rank
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE fk_imported_at_date BETWEEN 20240426 AND 20240502
    AND country='GB'
    AND delivery_wk_4 IS NOT NULL
GROUP BY 1,2,3,4
)


, VIEW_2B AS ( --- to get our base customers
SELECT DISTINCT fk_subscription
              , delivery_time
FROM VIEW_2A
WHERE rank=1
)

, VIEW_2C AS (
SELECT a.fk_subscription
       , a.delivery_time
       , CASE WHEN b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END AS status_wk_4
       , b.delivery_wk_4
       , (CASE WHEN b.delivery_wk_1='2024-05-27' THEN b.delivery_wk_1
           WHEN b.delivery_wk_2='2024-05-27' THEN b.delivery_wk_2
           WHEN b.delivery_wk_3='2024-05-27' THEN b.delivery_wk_3
           WHEN b.delivery_wk_4='2024-05-27' THEN b.delivery_wk_4
           WHEN b.upcoming_delivery_date='2024-05-27' THEN b.upcoming_delivery_date
           ELSE NULL END) AS delivery_date
       , (CASE
           WHEN b.delivery_wk_1='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_1 END)
           WHEN b.delivery_wk_2='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_2 END)
           WHEN b.delivery_wk_3='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_3 END)
           WHEN b.delivery_wk_4='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END)
           ELSE NULL END) AS status
       , SUBSTR(b.zip,1,LEN(b.zip)-3) AS zip
       , b.fk_imported_at_date
       , c.min_published_date AS published_date
FROM VIEW_2B AS a
LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.delivery_time = b.delivery_time
CROSS JOIN VIEW_1B AS c
WHERE b.fk_imported_at_date<=20240527
ORDER BY 1,8
)

--   SELECT * FROM VIEW_2C WHERE fk_imported_at_date>=20240524 AND (delivery_date!='2024-05-27' OR delivery_date IS NULL) ORDER BY 1/*GROUP BY 1,2 ORDER BY 1,2/*


, VIEW_3 AS (
SELECT a.fk_subscription
     , target_date
     , delivery_date
     , CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
     , CASE WHEN status_wk_4='cancelled' THEN 'cancelled' ELSE status END AS status --status_wk_4 AS status--
     , a.fk_imported_at_date
     , a.published_date
FROM VIEW_2C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip=b.postal_code
    AND a.delivery_time = b.origin_option_handle
WHERE (delivery_date='2024-05-27') OR (delivery_date IS NULL AND target_date IS NOT NULL AND status IS NOT NULL)
--    ((delivery_date!='2024-05-27' OR delivery_date IS NULL) AND status IS NOT NULL) --OR (delivery_date IS NULL AND status IS NOT NULL)
   --OR (delivery_date IS NULL AND status IS NOT NULL)
   --OR (target_date IS NOT NULL AND status IS NOT NULL) --AND fk_imported_at_date>20240426 AND fk_imported_at_date<20240524
ORDER BY 1,6
)


, VIEW_3B AS (
SELECT fk_subscription
     , MAX(CASE WHEN fk_imported_at_date<published_date THEN fk_imported_at_date END) AS prehmt
     , MIN(CASE WHEN fk_imported_at_date>=published_date THEN fk_imported_at_date END) AS hmt
FROM VIEW_3
GROUP BY 1
HAVING MIN(CASE WHEN fk_imported_at_date>=published_date THEN fk_imported_at_date END) IS NOT NULL
    AND MAX(CASE WHEN fk_imported_at_date<published_date THEN fk_imported_at_date END) IS NOT NULL
ORDER BY 1
)

, VIEW_4 AS (
SELECT DISTINCT a.fk_subscription
     , b.cohort
     , b.status
     , b.fk_imported_at_date
     , b.target_date
     , b.delivery_date
     , b.published_date
FROM VIEW_3B AS a
LEFT JOIN VIEW_3 AS b
    ON a.fk_subscription=b.fk_subscription
ORDER BY 1,4
)

SELECT cohort,status,count(distinct fk_subscription) FROM VIEW_4  WHERE fk_imported_at_date>20240524 GROUP BY 1,2 ORDER BY 1,2/*

SELECT cohort
     , status
     , COUNT(DISTINCT CASE WHEN fk_imported_at_date<published_date THEN fk_subscription END) prehmt
     , COUNT(DISTINCT CASE WHEN fk_imported_at_date>=published_date THEN fk_subscription END) hmt
     , COUNT(DISTINCT fk_subscription)
FROM VIEW_4
GROUP BY 1,2
ORDER BY 1,2

/*
SELECT cohort,
       --status,
       COUNT(DISTINCT fk_subscription) AS total,
       COUNT(DISTINCT CASE WHEN fk_imported_at_date<published_date THEN fk_subscription END) AS pre_hmt,
       COUNT(DISTINCT CASE WHEN fk_imported_at_date>=published_date THEN fk_subscription END) AS hmt
FROM VIEW_3
GROUP BY 1
ORDER BY 1


/*

WITH VIEW_1 AS ( --- to get the postal codes for a specific public holiday
SELECT DISTINCT business_unit,
       origin_option_handle,
       target_option_handle,
       origin_date,
       target_date,
       postal_code,
       REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
FROM public_holiday_shift_live.holiday_shift_latest
WHERE business_unit IN ('GB')
  AND origin_date='2024-05-27' --- UK Public Holiday
)

, VIEW_1B AS (
SELECT min(published_date) AS min_published_date
       , max(published_date) AS max_published_date
FROM VIEW_1
)

, VIEW_2A AS (
SELECT fk_subscription
     , delivery_wk_4
     , fk_imported_at_date
     , delivery_time
     , ROW_NUMBER() OVER(PARTITION BY fk_subscription ORDER BY fk_imported_at_date DESC) AS rank
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE fk_imported_at_date BETWEEN 20240426 AND 20240502
    AND country='GB'
    AND delivery_wk_4 IS NOT NULL
GROUP BY 1,2,3,4
)

, VIEW_2B AS (
SELECT DISTINCT fk_subscription
              , delivery_time
FROM VIEW_2A
WHERE rank=1
)


, VIEW_2C AS (
SELECT a.fk_subscription
       , a.delivery_time
       , CASE WHEN b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END AS status_wk_4
       , b.delivery_wk_4
       , (CASE WHEN b.delivery_wk_1='2024-05-27' THEN b.delivery_wk_1
           WHEN b.delivery_wk_2='2024-05-27' THEN b.delivery_wk_2
           WHEN b.delivery_wk_3='2024-05-27' THEN b.delivery_wk_3
           WHEN b.delivery_wk_4='2024-05-27' THEN b.delivery_wk_4
           ELSE NULL END) AS delivery_date
       , (CASE
           WHEN b.delivery_wk_1='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_1 END)
           WHEN b.delivery_wk_2='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_2 END)
           WHEN b.delivery_wk_3='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_3 END)
           WHEN b.delivery_wk_4='2024-05-27' THEN (CASE WHEN b.status_wk_0!='cancelled' AND b.status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE b.status_wk_4 END)
           ELSE NULL END) AS status
       , SUBSTR(b.zip,1,LEN(b.zip)-3) AS zip
       , b.fk_imported_at_date
       , c.min_published_date AS published_date
FROM VIEW_2B AS a
LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.delivery_time = b.delivery_time
CROSS JOIN VIEW_1B AS c
WHERE b.fk_imported_at_date<=20240527
ORDER BY 1,8
)


--SELECT * FROM VIEW_2C WHERE fk_imported_at_date LIKE '2024%' AND delivery_date='2024-05-27' AND status_wk_4='cancelled'
--/*

, VIEW_3A AS (
SELECT a.fk_subscription
     , target_date
     , delivery_date
     , CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
     , CASE WHEN status_wk_4='cancelled' THEN 'cancelled' ELSE status END AS status --status_wk_4 AS status--
     , CASE WHEN a.fk_imported_at_date<a.published_date THEN 1 END AS pre_HMT
     , CASE WHEN a.fk_imported_at_date>=a.published_date THEN 1 END AS HMT
     , a.fk_imported_at_date
     , a.published_date
FROM VIEW_2C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip=b.postal_code
    AND a.delivery_time = b.origin_option_handle
WHERE delivery_date='2024-05-27' AND fk_imported_at_date>20240426 AND fk_imported_at_date<20240524
ORDER BY 1,8
)

, VIEW_3B AS (
SELECT fk_subscription, MAX(pre_HMT) AS pre_HMT, MAX(HMT) AS HMT
FROM VIEW_3A
GROUP BY 1
HAVING MAX(pre_HMT)>0 AND MAX(HMT)>0
)

, VIEW_3C AS (
SELECT a.fk_subscription
     , b.target_date
     , b.delivery_date
     , b.cohort
     , b.status
     , b.fk_imported_at_date
     , b.published_date
FROM VIEW_3B AS a
LEFT JOIN VIEW_3A AS b
    ON a.fk_subscription=b.fk_subscription
)


--SELECT * FROM VIEW_3 WHERE delivery_date='2024-05-27'
--/*

SELECT cohort,
       status,
       COUNT(DISTINCT fk_subscription) AS total,
       COUNT(DISTINCT CASE WHEN fk_imported_at_date<published_date THEN fk_subscription END) AS pre_hmt,
       COUNT(DISTINCT CASE WHEN fk_imported_at_date>=published_date THEN fk_subscription END) AS hmt
FROM VIEW_3C
GROUP BY 1,2
ORDER BY 1,2
/*


, VIEW_3 AS (
SELECT DISTINCT fk_subscription
     , CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
     , status
     , fk_imported_at_date
     , b.published_date
     --, c.min_published_date
FROM VIEW_2B AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip=b.postal_code
    AND a.delivery_time = b.origin_option_handle
WHERE fk_imported_at_date>=20240401
ORDER BY 1,2,3,4
)


   SELECT * FROM VIEW_3 /*
