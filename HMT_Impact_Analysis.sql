--- HMT Analysis: Delivery Changes Due to Public Holidays and its Impact on Customer Behavior (with 1-Off changes incorporated) ---

-- Get the postal codes for a specific public holiday
WITH VIEW_1_HolidayShifts AS (
    SELECT DISTINCT business_unit
                  , origin_option_handle
                  , target_option_handle
                  , origin_date
                  , target_date
                  , postal_code
                  , REPLACE(SUBSTR(published_time, 1, 10), '-', '') AS published_date
    FROM public_holiday_shift_live.holiday_shift_standardized_ap
    WHERE meta.operation != 'd' ---< To filter out the deleted shifts
      AND business_unit IN ('GB') ---< Update the country
      AND origin_date='2024-04-01' ---< Update the Public Holiday
)


-- To get the start date of when HMT was made live for the public holiday above^
, VIEW_1B_HMTStartDate AS (
    SELECT MIN(published_date) AS published_date
    FROM VIEW_1_HolidayShifts
)


-- Get our base customers/subscriptions that have the public holiday delivery
, VIEW_2_Subscriptions AS (
SELECT *
FROM (
    SELECT country
        , fk_subscription
        , delivery_wk_4
        , fk_imported_at_date
        , delivery_time
        , zip AS zipcode
        , CASE WHEN country IN ('GB') THEN SUBSTR(zip,1,LEN(REPLACE(zip,' ',''))-3)
             WHEN country IN ('IE') THEN SUBSTR(zip,1,LEN(zip)-4)
             WHEN country IN ('NL') AND REPLACE(UPPER(zip), ' ','')=6
                 THEN SUBSTR(zip,1,LEN(zip)-2)
             WHEN country IN ('NL') AND zip='NL5042ZK'
                 THEN LEFT('5042ZK',4)
            ELSE zip END AS zip
        , ROW_NUMBER() OVER(PARTITION BY fk_subscription ORDER BY fk_imported_at_date) AS rank
    FROM scm_forecasting_model.delivery_snapshots
    WHERE fk_imported_at_date BETWEEN 20240301 AND 20240307 ---< Update the relevant dates
      AND country IN ('GB') ---< Update the country
      AND delivery_wk_4 BETWEEN '2024-03-30' AND '2024-04-05' ---< Update the relevant delivery weeks
    GROUP BY 1,2,3,4,5,6)
WHERE rank=1 AND LENGTH(zip)>2
)


/* This serves as a checker only to filter the specific delivery weeks and dates for VIEW_2_Subscriptions^
SELECT DISTINCT delivery_wk_4
FROM scm_forecasting_model.subscription_forecast_snapshots
WHERE country='GB'
  --AND delivery_wk_4= '2025-01-01'
  AND fk_imported_at_date BETWEEN 20241129 AND 20241205
ORDER BY 1
*/

-- To get the data with 1-off changes
, VIEW_3_1OFF AS (
    SELECT DISTINCT scs.business_unit
                  , scs.fk_subscription
                  , scs.delivery_weekday AS changed_delivery_weekday
                  , dd.day_name
                  , scs.delivery_time AS changed_delivery_time
                  , dd.date_string_backwards AS changed_delivery_date
                  , scs.week_id AS hellofresh_week
    FROM dl_bob_live_non_pii.subscription_change_schedule AS scs
    LEFT JOIN dimensions.date_dimension AS dd
        ON scs.week_id = dd.hellofresh_week
        AND scs.delivery_weekday = (dd.day_of_week + 1)  -- In date dimensions table, Monday is 0. In SCS table, Monday is 1, so we have do to +1
    WHERE scs.business_unit IN ('GB') ---< Update the country
      AND scs.week_id >= '2024-W01'
      AND scs.delivery_time IS NOT NULL
)


-- To join the subscriptions data with 1-off changes incorporated
, VIEW_4_Subscriptionswith1OFF AS (
    SELECT DISTINCT ss.country
        , ss.fk_subscription
        , ss.zip
        , dd.hellofresh_week
        --, off.changed_delivery_date
        --, ss.delivery_wk_4
        , off.changed_delivery_time
        , ss.delivery_time
        , COALESCE(off.changed_delivery_time,ss.delivery_time) AS updated_delivery_time
        , COALESCE(off.changed_delivery_date,ss.delivery_wk_4) AS updated_delivery_date
        , ss.fk_imported_at_date
    FROM VIEW_2_Subscriptions AS ss
    LEFT JOIN dimensions.date_dimension AS dd
        ON ss.delivery_wk_4 = dd.date_string_backwards
    LEFT JOIN VIEW_3_1OFF AS off
        ON ss.country = off.business_unit
        AND ss.fk_subscription = off.fk_subscription
        AND dd.hellofresh_week = off.hellofresh_week
    WHERE ss.fk_imported_at_date IS NOT NULL
)


-- To join the HMT shifted postal codes with the subscriptions data with 1-off changes incorporated
, VIEW_5_HMTShiftsSubscriptions1OFF AS (
    SELECT DISTINCT hs.business_unit AS country
                  , hs.postal_code
                  , hs.origin_option_handle
                  , hs.target_option_handle
                  , hs.origin_date
                  , COALESCE(hs.target_date,COALESCE(ssoff.updated_delivery_date, hs.origin_date)) AS target_date
                  , ssoff.fk_subscription
                  , ssoff.zip
                  , ssoff.updated_delivery_time
                  , ssoff.updated_delivery_date
                  , COALESCE(ssoff.updated_delivery_date, hs.origin_date) AS updated_origin_date
    FROM VIEW_4_Subscriptionswith1OFF AS ssoff
    LEFT JOIN VIEW_1_HolidayShifts AS hs
        ON ssoff.country = hs.business_unit
        AND ssoff.zip = hs.postal_code
        AND ssoff.updated_delivery_time = hs.origin_option_handle
        AND ssoff.updated_delivery_date = hs.origin_date
    LEFT JOIN dimensions.date_dimension AS dd
        ON ssoff.updated_delivery_date = dd.date_string_backwards
)


-- Categorise the customers
, VIEW_6_CustomerCohorts AS (
    SELECT country
        , fk_subscription
        , zip
        , updated_origin_date AS original_delivery_date
        , target_date AS new_delivery_date
        , (CASE WHEN updated_origin_date=target_date THEN 'Cohort A (0)' --- non-shifted: delivery date is on the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=-1 THEN 'Cohort B (-1)'  --- shifted: delivery date is shifted to 1 day earlier than the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=1 THEN 'Cohort C (+1)' --- shifted: delivery date is shifted to 1 day later than the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=-2 THEN 'Cohort D (-2)' --- shifted: delivery date is shifted to 2 days earlier than the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=2 THEN 'Cohort E (+2)' --- shifted: delivery date is shifted to 2 days later than the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=-3 THEN 'Cohort F (-3)' --- shifted: delivery date is shifted to 3 days earlier than the public holiday
                WHEN DATEDIFF(target_date, updated_origin_date)=3 THEN 'Cohort G (+3)' --- shifted: delivery date is shifted to 3 days later than the public holiday
            END) AS cohort
    FROM VIEW_5_HMTShiftsSubscriptions1OFF
    WHERE updated_origin_date='2024-04-01' ---< Update the PH
)

--SELECT cohort, COUNT(DISTINCT fk_subscription) FROM VIEW_2C GROUP BY 1 ORDER BY 1/*


-- Grab the status for these customers from the first date we used (in VIEW_2_Subscriptions CTE)
-- First set up the unique list of customers
-- Second, cross join for every day of the month
, VIEW_7_UniqueCustomersDailyStatus AS (
    SELECT cc.fk_subscription
        , ss.fk_imported_at_date
    FROM VIEW_6_CustomerCohorts AS cc
    CROSS JOIN (
        SELECT DISTINCT fk_imported_at_date
        FROM scm_forecasting_model.subscription_forecast_snapshots
        WHERE fk_imported_at_date BETWEEN 20240301 AND 20240406  ---< Update the relevant dates
         AND country IN ('GB')  ---< Update the country
        GROUP BY 1) AS ss
    ORDER BY 1,2
)


-- Third, pull info for the week the PH falls on, moving WOW
, VIEW_8_PHCustomersDailyStatus AS (
    SELECT ss.country
        , ucds.*
          ---v Update the relevant start date to include in the analysis:
        , (CASE WHEN DATEDIFF(TO_DATE(CAST(ucds.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-03-01') BETWEEN 0 AND 6 THEN status_wk_4
                WHEN DATEDIFF(TO_DATE(CAST(ss.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-03-01') BETWEEN 7 AND 13 THEN status_wk_3
                WHEN DATEDIFF(TO_DATE(CAST(ss.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-03-01') BETWEEN 14 AND 20 THEN status_wk_2
                WHEN DATEDIFF(TO_DATE(CAST(ss.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-03-01') BETWEEN 21 AND 27 THEN status_wk_1
                WHEN DATEDIFF(TO_DATE(CAST(ss.fk_imported_at_date AS STRING), 'yyyyMMdd'), '2024-03-01') BETWEEN 28 AND 34 THEN status_wk_0
            ELSE 'Other' END) AS status
    FROM VIEW_7_UniqueCustomersDailyStatus AS ucds
    LEFT JOIN scm_forecasting_model.subscription_forecast_snapshots AS ss
        ON ucds.fk_subscription=ss.fk_subscription
        AND ucds.fk_imported_at_date=ss.fk_imported_at_date
    WHERE ss.country IN ('GB')  ---< Update the country
)


--- To attach the cohort categorization with the customers with public holiday as their delivery date data
, VIEW_9_PHCustomersCohorts AS (
SELECT pcds.*
     , cc.cohort
     --, CASE WHEN B.cohort IS NULL THEN 'Cohort A (0)' ELSE B.cohort END AS cohort
FROM VIEW_8_PHCustomersDailyStatus AS pcds
LEFT JOIN VIEW_6_CustomerCohorts AS cc
    ON pcds.fk_subscription=cc.fk_subscription
)


--- To get the daily total subscriptions per cohort
, VIEW_10_TotalCustomers AS (
SELECT fk_imported_at_date
    , cohort
    , COUNT(DISTINCT(fk_subscription)) AS Total_Subs
FROM VIEW_9_PHCustomersCohorts
GROUP BY 1,2
ORDER BY 2,1
)


-- Summarise the data
, VIEW_5_Summary AS (
SELECT pc.fk_imported_at_date AS Date
    , pc.status AS Status
    , COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort A (0)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_A
    , COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort B (-1)' THEN pc.fk_subscription END) /tc.Total_Subs AS Cohort_B
    , COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort C (+1)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_C
    , COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort D (-2)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_D
    , COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort E (+2)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_E
    --, COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort F (-3)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_F
    --, COUNT(DISTINCT CASE WHEN pc.cohort = 'Cohort G (+3)' THEN pc.fk_subscription END) / tc.Total_Subs AS Cohort_G
FROM VIEW_9_PHCustomersCohorts AS pc
LEFT JOIN VIEW_10_TotalCustomers AS tc
    ON pc.fk_imported_at_date = tc.fk_imported_at_date
    AND pc.cohort = tc.cohort
GROUP BY 1,2,pc.cohort,tc.Total_Subs
ORDER BY 2,1
)


SELECT Date
     , Status
     , MAX(Cohort_A) AS Cohort_A
     , MAX(Cohort_B) AS Cohort_B
     , MAX(Cohort_C) AS Cohort_C
     , MAX(Cohort_D) AS Cohort_D
     , MAX(Cohort_E) AS Cohort_E
     --, MAX(Cohort_F) AS Cohort_F
     --, MAX(Cohort_G) AS Cohort_G
     , published_date AS HMT_Date
FROM VIEW_5_Summary
CROSS JOIN VIEW_1B_HMTStartDate
GROUP BY 1,2,8
ORDER BY 1,2
