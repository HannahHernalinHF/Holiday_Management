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