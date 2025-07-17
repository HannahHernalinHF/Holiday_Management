A. The HMT_Tracker_Dashboard.sql - raw source for the table stated below. Note that this is not the actual source of the HMT Tracker Dashboard, but rather the pipeline responsible for creating the enriched table that serves as the source for the dashboard.

  Link to the pipeline: https://github.com/hellofresh/tardis-community/tree/master/pipelines/no-tribe/shared/strategy-and-insights/pipeline-logistics-holiday-management (Enriched Staging Table: public_strategy_and_insights_staging.logistics_hmt_tracker_enriched)

  Link to the dashboard: https://tableau.hellofresh.io/#/views/HMTTrackerWIP/HMTImpactOverview?:iid=1


B. The HMT_Impact_Analysis.sql script calculates the daily pause, cancellation, and active rates for customers with deliveries scheduled on a public holiday, comparing them to customers whose delivery dates were originally on a public holiday but were shifted by the HMT.

C. HMT_Impact_with_Delivery_Options.sql script calculates the daily pause, cancellation, and active rates for customers with deliveries scheduled on a public holiday as well as shifts in other delivery options, comparing them to customers whose delivery dates were originally on a public holiday but were shifted by the HMT. 

D. HMT_Impact_AOR.sql script calculates the pre-5W, pre-10W, post-5W, and post-10W AORs for customers with deliveries scheduled on a public holiday as well as shifts in other delivery options, comparing them to customers whose delivery dates were originally on a public holiday but were shifted by the HMT. 

E. AOR_Change_Comparison.py - measures the 5W-10W AOR performance of non-shifted vs shifted customers.
