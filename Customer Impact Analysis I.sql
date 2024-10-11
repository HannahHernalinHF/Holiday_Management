--- Customer Impact Analysis: Delivery Changes Due to Public Holidays

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

/*
, VIEW_1B AS ( --- to get the start and cutoff date of HMT
SELECT MIN(published_date) AS min_published_date,
       MAX(published_date) AS max_published_date
FROM VIEW_1
)*/

/*
, VIEW_2A AS ( --- to get the subscriptions that are between 25-31 May 2024
SELECT country
  , zip
  --, region
  , fk_subscription
  , fk_imported_at_date
  , delivery_time
  , status_wk_0
  , (CASE WHEN delivery_wk_1 BETWEEN '2024-03-30' AND '2024-04-06' THEN delivery_wk_1 -- BETWEEN '2024-05-25' AND '2024-05-31'
    WHEN delivery_wk_2 BETWEEN '2024-03-30' AND '2024-04-06' THEN delivery_wk_2
    WHEN delivery_wk_3 BETWEEN '2024-03-30' AND '2024-04-06' THEN delivery_wk_3
    WHEN delivery_wk_4 BETWEEN '2024-03-30' AND '2024-04-06' THEN delivery_wk_4
    WHEN delivery_wk_0 BETWEEN '2024-03-30' AND '2024-04-06' THEN delivery_wk_0
    ELSE NULL END) AS delivery_var
  , (CASE
    WHEN delivery_wk_1 BETWEEN '2024-03-30' AND '2024-04-06' THEN (CASE WHEN status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE status_wk_1 END)
    WHEN delivery_wk_2 BETWEEN '2024-03-30' AND '2024-04-06' THEN (CASE WHEN status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE status_wk_2 END)
    WHEN delivery_wk_3 BETWEEN '2024-03-30' AND '2024-04-06' THEN (CASE WHEN status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE status_wk_3 END)
    WHEN delivery_wk_4 BETWEEN '2024-03-30' AND '2024-04-06' THEN (CASE WHEN status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE status_wk_4 END)
    WHEN delivery_wk_0 BETWEEN '2024-03-30' AND '2024-04-06' THEN (CASE WHEN status_wk_0 LIKE 'skipped%' THEN 'paused' ELSE status_wk_0 END)
    ELSE NULL END) AS status_var
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country='GB' --AND fk_imported_at_date>=20240101 AND fk_imported_at_date<=20240527
)
*/

, VIEW_2A AS ( --- to get the subscriptions with the delivery date on the public holiday
SELECT country
  , zip
  , fk_subscription
  , delivery_time
  , fk_imported_at_date
  , (CASE
      WHEN delivery_wk_1='2024-05-27' THEN delivery_wk_1
      WHEN delivery_wk_2='2024-05-27' THEN delivery_wk_2
      WHEN delivery_wk_3='2024-05-27' THEN delivery_wk_3
      WHEN delivery_wk_4='2024-05-27' THEN delivery_wk_4
    ELSE NULL END) AS delivery_date
  , (CASE WHEN status_wk_0='cancelled' THEN 'cancelled'
      WHEN delivery_wk_1='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE status_wk_1 END)
      WHEN delivery_wk_2='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE status_wk_2 END)
      WHEN delivery_wk_3='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE status_wk_3 END)
      WHEN delivery_wk_4='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE status_wk_4 END)
    ELSE NULL END) AS status
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country='GB'
)
/*

, VIEW_3 AS (
SELECT DISTINCT CASE WHEN target_date IS NULL THEN 'Cohort A (0)'  --- non-shifted: delivery date is on the public holiday
    WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
    WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
    WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
    WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
    , status
    , CASE WHEN fk_imported_at_date < 20240514 THEN 'pre-HMT' WHEN fk_imported_at_date >= 20240514 THEN 'HMT' END AS hmt_status
    , fk_subscription--, COUNT(DISTINCT fk_subscription) AS count
FROM VIEW_2A AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip LIKE CONCAT(b.postal_code,'%')
    AND a.delivery_time = b.origin_option_handle
    AND a.country = b.business_unit
WHERE delivery_date='2024-05-27' AND status IS NOT NULL
--GROUP BY 1,2,3
--ORDER BY 1,2,3
)


, VIEW_3B AS (
SELECT cohort
    , status
    , COUNT(DISTINCT CASE WHEN hmt_status='HMT' THEN fk_subscription END) AS hmt_count --, SUM(CASE WHEN hmt_status='HMT' THEN count END) AS hmt_count
    , COUNT(DISTINCT CASE WHEN hmt_status='pre-HMT' THEN fk_subscription END) AS pre_hmt_count --, SUM(CASE WHEN hmt_status='pre-HMT' THEN count END) AS pre_hmt_count
FROM VIEW_3
GROUP BY 1,2
ORDER BY 1,2
)


, VIEW_4 AS ( --- to calculate the total subscriptions per cohort
SELECT cohort
       , SUM(hmt_count) AS total_hmt --, SUM(CASE WHEN hmt_status='HMT' THEN count END) AS total_hmt
       , SUM(pre_hmt_count) AS total_pre_hmt --, SUM(CASE WHEN hmt_status='pre-HMT' THEN count END) AS total_pre_hmt  --
FROM VIEW_3B
GROUP BY 1
)

, VIEW_5 AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , a.pre_hmt_count
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.hmt_count/total_pre_hmt WHEN a.status='cancelled' THEN a.hmt_count/b.total_pre_hmt END)*100,2),'%') AS hmt_cancellation_pause_rate
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.pre_hmt_count/total_pre_hmt WHEN a.status='cancelled' THEN a.pre_hmt_count/b.total_pre_hmt END)*100,2),'%')  AS pre_hmt_cancellation_pause_rate
FROM VIEW_3B AS a
LEFT JOIN VIEW_4 AS b
    ON a.cohort=b.cohort
ORDER BY 1,2
)

SELECT * FROM VIEW_5

*/

--SELECT * FROM VIEW_2A WHERE delivery_date='2024-05-27' AND status ='cancelled' ORDER BY 5 /*

, VIEW_2B AS ( --- to get the subscriptions on the public holiday
SELECT a.fk_subscription
       , a.zip
       , a.delivery_time
       , a.delivery_date
       , MAX(CASE WHEN a.fk_imported_at_date >= b.min_published_date THEN a.fk_imported_at_date END) AS hmt_date
       , MAX(CASE WHEN a.fk_imported_at_date >= b.min_published_date THEN a.status END) AS hmt_status
       , MAX(CASE WHEN a.fk_imported_at_date < b.min_published_date THEN a.fk_imported_at_date END) AS pre_hmt_date
       , MAX(CASE WHEN a.fk_imported_at_date < b.min_published_date  THEN a.status END) AS pre_hmt_status
FROM VIEW_2A AS a
CROSS JOIN (SELECT min(published_date) AS min_published_date FROM VIEW_1) AS b
WHERE delivery_date='2024-05-27'
GROUP BY 1,2,3,4
ORDER BY 1,2
)

, VIEW_2C AS ( --- to update the paused subscriptions pre-HMT; to ensure that those were paused pre-HMT are not included in the HMT count
SELECT DISTINCT fk_subscription,
       zip,
       delivery_time,
       delivery_date,
       hmt_date,
       pre_hmt_date,
       CASE WHEN hmt_status='paused' AND pre_hmt_status='paused' THEN 'pre-paused' ELSE hmt_status END AS hmt_status,
       CASE WHEN pre_hmt_status IS NULL AND hmt_status IS NOT NULL THEN 'running' ELSE pre_hmt_status END AS pre_hmt_status
FROM VIEW_2B
)


, VIEW_3A AS ( --- to group into cohorts, calculate the subscriptions that were impacted by HMT vs the pre-HMT
SELECT CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
    , CASE WHEN hmt_status='pre-paused' THEN 'paused' ELSE hmt_status END AS status
    , COUNT(DISTINCT CASE WHEN hmt_status!='pre-paused' THEN a.fk_subscription END) AS hmt_count
FROM VIEW_2C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip LIKE CONCAT(b.postal_code,'%')
    AND a.delivery_time = b.origin_option_handle
WHERE hmt_status IS NOT NULL AND pre_hmt_status IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
)


, VIEW_3B AS ( --- to group into cohorts, calculate the active, cancelled, and paused subscriptions that were impacted by HMT vs the pre-HMT
SELECT CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)' --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
    , pre_hmt_status
    , COUNT(DISTINCT a.fk_subscription) AS pre_hmt_count
FROM VIEW_2C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip LIKE CONCAT(b.postal_code,'%')
    AND a.delivery_time = b.origin_option_handle
WHERE hmt_status IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
)


, VIEW_5A AS ( --- to calculate the total subscriptions per cohort
SELECT a.cohort
       , SUM(hmt_count) AS total_hmt
       , SUM(pre_hmt_count) AS total_pre_hmt
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
GROUP BY 1
)

, VIEW_6 AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , b.pre_hmt_count
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.hmt_count/total_pre_hmt WHEN a.status='cancelled' THEN a.hmt_count/total_pre_hmt END)*100,2),'%') AS hmt_cancellation_pause_rate
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN b.pre_hmt_count/total_pre_hmt WHEN a.status='cancelled' THEN b.pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_cancellation_pause_rate
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
LEFT JOIN VIEW_5A AS c
    ON a.cohort=c.cohort
ORDER BY 1,2
)

SELECT * FROM VIEW_6
/*

/*
, VIEW_4A AS ( --- customers expecting boxes on the public holiday
SELECT DISTINCT fk_subscription, delivery_time
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country = 'GB'
  AND (
      (delivery_wk_4 = '2024-05-27' AND status_wk_4 = 'running')
          OR (delivery_wk_3 = '2024-05-27' AND status_wk_3 = 'running')
          OR (delivery_wk_2 = '2024-05-27' AND status_wk_2 = 'running')
          OR (delivery_wk_1 = '2024-05-27' AND status_wk_1 = 'running')
      )
)

, VIEW_4B AS ( --- cancelled subscriptions pre-HMT vs HMT
  SELECT DISTINCT country
       , zip
       , fk_subscription
       , delivery_time
       , canceled_at
       , CASE WHEN date_format(canceled_at, 'yyyyMMdd') < min_published_date THEN 'cancelled pre-HMT'
           WHEN  date_format(canceled_at, 'yyyyMMdd') >= min_published_date /*AND max_published_date*/ THEN 'cancelled' END AS status
  FROM scm_forecasting_model.subscription_forecast_snapshots AS a
  CROSS JOIN VIEW_1B
  WHERE country = 'GB'
    AND status_wk_0 = 'cancelled'
    AND fk_imported_at_date = 20240826
    AND canceled_at>='2024-01-01 00:00:00' AND date_format(canceled_at, 'yyyyMMdd') >= min_published_date
  )


, VIEW_4C AS ( --- to join the subscriptions with the delivery date on the public holiday and their cancellation status
SELECT b.*,
       '2024-05-27' AS delivery_date
FROM VIEW_4A AS a
INNER JOIN VIEW_4B AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.delivery_time = b.delivery_time
)

, VIEW_4D AS ( --- to group into cohorts, calculate the cancelled subscriptions due to HMT vs the pre-HMT
SELECT CASE WHEN target_date IS NULL THEN 'Cohort A (0)'
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)'
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)'
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort
    , 'cancelled' AS status
    , COUNT(DISTINCT CASE WHEN status='cancelled' THEN fk_subscription END) AS hmt_count
    , COUNT(DISTINCT CASE WHEN status='cancelled pre-HMT' THEN fk_subscription END) AS pre_hmt_count
FROM VIEW_4C AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip LIKE CONCAT(b.postal_code,'%')
    AND a.delivery_time = b.origin_option_handle
WHERE status IS NOT NULL
GROUP BY 1,2
ORDER BY 1,2
)


, VIEW_5A AS ( --- to calculate the total subscriptions per cohort including running, paused, and cancelled
SELECT a.cohort,
       SUM(hmt_count) AS total_hmt,
       SUM(pre_hmt_count) AS total_pre_hmt
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
GROUP BY 1
)

, VIEW_5B AS ( --- to calculate the total subscriptions per cohort including running, paused, and cancelled
SELECT cohort,
       SUM(hmt_count) AS total_hmt,
       SUM(pre_hmt_count) AS total_pre_hmt
FROM VIEW_4D
GROUP BY 1
)

, VIEW_5C AS (
SELECT cohort,
       SUM(total_hmt) AS total_hmt,
       SUM(total_pre_hmt) AS total_pre_hmt
FROM (SELECT * FROM VIEW_5A
               UNION
               SELECT * FROM VIEW_5B)
GROUP BY 1
ORDER BY 1
    )


, VIEW_6 AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , b.pre_hmt_count
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.hmt_count/total_hmt END)*100,2),'%') AS hmt_pause_rate
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN b.pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_pause_rate
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
LEFT JOIN VIEW_5C AS c
    ON a.cohort=c.cohort
ORDER BY 1,2
)

, VIEW_7 AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , a.pre_hmt_count
       , CONCAT(ROUND((a.hmt_count/b.total_hmt)*100,2),'%') AS hmt_cancellation_rate
       , CONCAT(ROUND((a.pre_hmt_count/b.total_pre_hmt)*100,2),'%')  AS pre_hmt_cancellation_rate
FROM VIEW_4D AS a
LEFT JOIN VIEW_5C AS b
    ON a.cohort=b.cohort
ORDER BY 1,2
)


SELECT * FROM VIEW_6
UNION
SELECT * FROM VIEW_7
ORDER BY 1,2

/*

, VIEW_6A AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , b.pre_hmt_count
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
ORDER BY 1,2
)

, VIEW_6B AS (
    SELECT * FROM VIEW_6A
    UNION
    SELECT * FROM VIEW_5D
)

, VIEW_7 AS (
    SELECT *
       , CONCAT(ROUND((CASE WHEN status='paused' THEN hmt_count/total_pre_hmt END)*100,2),'%') AS hmt_pause_rate
       , CONCAT(ROUND((CASE WHEN status='paused' THEN pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_pause_rate
       , CONCAT(ROUND((CASE WHEN status='cancelled' THEN hmt_count/total_pre_hmt END)*100,2),'%') AS hmt_cnc_rate
       , CONCAT(ROUND((CASE WHEN status='cancelled' THEN pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_cnc_rate
    FROM VIEW_6B AS a
    LEFT JOIN VIEW_4 AS c
        ON a.cohort=c.cohort
    ORDER BY 1,2
)

 SELECT * FROM VIEW_7 /*


, VIEW_6 AS (
SELECT a.cohort
       , a.status
       , a.hmt_count
       , b.pre_hmt_count
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.hmt_count/total_pre_hmt END)*100,2),'%') AS hmt_pause_rate
       , CONCAT(ROUND((CASE WHEN a.status='paused' THEN b.pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_pause_rate
FROM VIEW_3A AS a
LEFT JOIN VIEW_3B AS b
    ON a.cohort=b.cohort
    AND a.status=b.pre_hmt_status
LEFT JOIN VIEW_4 AS c
    ON a.cohort=c.cohort
ORDER BY 1,2
)

, VIEW_7 AS (
SELECT *
     , CONCAT(ROUND((CASE WHEN a.status='cancelled' THEN a.hmt_count/total_pre_hmt END)*100,2),'%') AS hmt_cancellation_rate
     , CONCAT(ROUND((CASE WHEN a.status='paused' THEN a.pre_hmt_count/total_pre_hmt END)*100,2),'%')  AS pre_hmt_cancellation_rate
FROM VIEW_5D AS a
LEFT JOIN VIEW_4 AS b
    ON a.cohort=b.cohort
)

, VIEW_8 AS (
SELECT * FROM VIEW_6
UNION
SELECT * FROM VIEW_7
)


SELECT *
FROM VIEW_8
ORDER BY 1,2
