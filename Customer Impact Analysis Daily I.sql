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

, VIEW_1B AS (
SELECT min(published_date) AS min_published_date
--       , max(published_date) AS max_published_date
FROM VIEW_1
)

, VIEW_2A AS ( --- to get the subscriptions with the delivery date on the public holiday
SELECT country
  , SUBSTR(zip,1,LEN(zip)-3) AS zip_clean
  , fk_subscription
  , delivery_time
  , fk_imported_at_date
  , (CASE
      WHEN delivery_wk_0='2024-05-27' THEN delivery_wk_0
      WHEN delivery_wk_1='2024-05-27' THEN delivery_wk_1
      WHEN delivery_wk_2='2024-05-27' THEN delivery_wk_2
      WHEN delivery_wk_3='2024-05-27' THEN delivery_wk_3
      WHEN delivery_wk_4='2024-05-27' THEN delivery_wk_4
    ELSE NULL END) AS delivery_date
  , (CASE WHEN status_wk_0='cancelled' THEN 'cancelled'
      WHEN delivery_wk_0='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_0 LIKE 'skipped%' THEN 'paused' ELSE status_wk_0 END)
      WHEN delivery_wk_1='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_1 LIKE 'skipped%' THEN 'paused' ELSE status_wk_1 END)
      WHEN delivery_wk_2='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_2 LIKE 'skipped%' THEN 'paused' ELSE status_wk_2 END)
      WHEN delivery_wk_3='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_3 LIKE 'skipped%' THEN 'paused' ELSE status_wk_3 END)
      WHEN delivery_wk_4='2024-05-27' THEN (CASE WHEN status_wk_0!='cancelled' AND status_wk_4 LIKE 'skipped%' THEN 'paused' ELSE status_wk_4 END)
    ELSE NULL END) AS status
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country='GB'
)


, VIEW_2B AS (
SELECT *
FROM VIEW_2A
WHERE delivery_date='2024-05-27'
    )

, VIEW_3 AS (
SELECT DISTINCT fk_subscription
     , CASE WHEN target_date IS NULL THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
            WHEN RIGHT(target_date,2)-RIGHT(delivery_date,2)=2 THEN 'Cohort E (+2)' END AS cohort --- shifted: delivery date is shifted to 2 days later than the public holiday
     , status --, CASE WHEN fk_imported_at_date<c.min_published_date AND status='paused' THEN 'pre-paused' ELSE status END AS status
     , fk_imported_at_date
     --, b.published_date
     , c.min_published_date AS published_date
FROM VIEW_2B AS a
LEFT JOIN VIEW_1 AS b
    ON a.zip_clean=b.postal_code
    AND a.delivery_time = b.origin_option_handle
CROSS JOIN VIEW_1B AS c
ORDER BY 1,2,3,4
)


SELECT cohort,
       status,
       fk_imported_at_date,
       COUNT(DISTINCT fk_subscription)
FROM VIEW_3
GROUP BY 1,2,3
ORDER BY 1,2,3

/*

/*
, VIEW_4 AS ( --- paused pre-HMT and/or paused on/after HMT was made live.
SELECT fk_subscription,
       cohort,
       MAX(CASE WHEN fk_imported_at_date < min_published_date AND status = 'paused' THEN 1 ELSE 0 END) AS paused_pre_hmt,
       MAX(CASE WHEN fk_imported_at_date >= min_published_date AND status = 'paused' THEN 1 ELSE 0 END) AS paused_after_hmt
FROM VIEW_3 AS a
GROUP BY 1,2
ORDER BY 1
)*/

, VIEW_4A AS (
SELECT fk_subscription,
    cohort,
    MIN(status) AS min_hmt_status,
    MIN(fk_imported_at_date) AS min_hmt_date
FROM VIEW_3
WHERE fk_imported_at_date>=min_published_date
GROUP BY 1,2
ORDER BY 1,2
)

, VIEW_4B AS (
SELECT fk_subscription,
    cohort,
    MAX(status) AS max_pre_hmt_status,
    MAX(fk_imported_at_date) AS max_pre_hmt_date
FROM VIEW_3
WHERE fk_imported_at_date<min_published_date
GROUP BY 1,2
ORDER BY 1,2
)

, VIEW_4C AS (
SELECT b.*, a.min_hmt_date,a.min_hmt_status
FROM VIEW_4A AS a
LEFT JOIN VIEW_4B AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.cohort = b.cohort
WHERE(a.min_hmt_status='paused' AND b.max_pre_hmt_status!='paused')
    OR (a.min_hmt_status!='paused' AND b.max_pre_hmt_status='paused')
    )

SELECT * FROM VIEW_4C WHERE max_pre_hmt_status='paused'


   /*

, VIEW_5 AS (
SELECT *
FROM VIEW_4
--WHERE (max_pre_hmt_status='paused' AND min_hmt_status!='paused')
ORDER BY 1
)


SELECT *
FROM VIEW_5
WHERE max_pre_hmt_status='paused' AND min_hmt_status!='paused'

/*

SELECT *
FROM VIEW_5
WHERE max_pre_hmt_status='paused'
/*


SELECT --a.fk_imported_at_date,
       a.cohort,
       a.status,
       COUNT(DISTINCT CASE WHEN a.fk_imported_at_date<min_published_date THEN a.fk_subscription END) AS pre_hmt_count,
       COUNT(DISTINCT CASE WHEN a.fk_imported_at_date>=min_published_date THEN a.fk_subscription END) AS hmt_count
FROM VIEW_3 AS a
INNER JOIN VIEW_4 AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.cohort = b.cohort
--WHERE NOT (max_pre_hmt_status='paused' AND min_hmt_status='paused')
--  AND (max_pre_hmt_status IS NOT NULL OR min_hmt_status IS NOT NULL)
/*
  OR (max_pre_hmt_status='paused' AND min_hmt_status!='paused')
  OR (max_pre_hmt_status!='paused' AND min_hmt_status='paused')*/
GROUP BY 1,2
ORDER BY 1,2

/*
SELECT a.fk_subscription,
       a.cohort,
       a.status,
       a.fk_imported_at_date,
       a.published_date
FROM VIEW_3 AS a
LEFT JOIN VIEW_5 AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.cohort = b.cohort
WHERE (max_pre_hmt_status!='paused' AND min_hmt_status!='paused')
  OR (max_pre_hmt_status='paused' AND min_hmt_status!='paused')
  OR (max_pre_hmt_status!='paused' AND min_hmt_status='paused')
GROUP BY 1,2
ORDER BY 1,2

/*

SELECT *
FROM VIEW_5
WHERE max_pre_hmt_status='paused' AND min_hmt_status='paused'


/*

SELECT
    a.cohort,
    a.status,
    COUNT(DISTINCT CASE WHEN a.fk_imported_at_date<min_published_date THEN a.fk_subscription END) AS pre_hmt_count,
    COUNT(DISTINCT CASE WHEN a.fk_imported_at_date>=min_published_date THEN a.fk_subscription END) AS hmt_count
FROM VIEW_3 AS a
LEFT JOIN VIEW_4 AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.cohort = b.cohort
GROUP BY 1,2
ORDER BY 1,2

/*

SELECT a.cohort,
       a.status,
       CASE WHEN fk_imported_at_date<min_published_date THEN 'pre-HMT'
           WHEN fk_imported_at_date>=min_published_date THEN 'HMT'
               END AS type,
       COUNT(DISTINCT a.fk_subscription)
    /*a.fk_subscription,
    a.cohort,
    a.status,
    a.fk_imported_at_date,
    a.published_date*/
FROM VIEW_3 AS a
LEFT JOIN VIEW_4 AS b
    ON a.fk_subscription = b.fk_subscription
    AND a.cohort = b.cohort
WHERE   -- Keep counting paused statuses if before HMT was made live
  (fk_imported_at_date < min_published_date)
  -- Stop counting paused subscriptions if they were already paused before HMT was made live and continued to be paused after
  OR (fk_imported_at_date >= min_published_date AND status != 'paused')
  -- Count other statuses normally
  OR (fk_imported_at_date >= min_published_date AND status = 'paused' AND (b.paused_pre_hmt = 0 OR b.paused_after_hmt = 0))
GROUP BY 1,2,3
ORDER BY 1,2,3
