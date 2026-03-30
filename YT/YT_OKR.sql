DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.yt_bfm;


DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);

DEFINE MACRO BOQ DATE_TRUNC($1,QUARTER);

DEFINE MACRO DAYS_REMAINING_IN_QUARTER DATE_DIFF($EOQ(max_date),max_date,DAY);

DEFINE MACRO DAYS_QTD DATE_DIFF(max_date,$BOQ(max_date),DAY);

DEFINE MACRO MAX_AS MAX($1) AS $1;

DEFINE MACRO EOQ DATE_ADD(DATE_TRUNC(DATE_ADD($1, INTERVAL 1 QUARTER), QUARTER), INTERVAL -1 DAY);

DEFINE MACRO QUERY

WITH
Mapped AS
(
SELECT
  DISTINCT
    service_country_code AS service_country_code,
    product_group AS product_group,
    $GEOSECTOR(sector_name, service_country_code) AS geosector,
    $VERTICAL(service_country_code, sector_name, sub_sector_name) AS vertical,
    -- company_rollup.parent_name AS parent_name,
    -- company_rollup.parent_id AS parent_id,
    -- company_rollup.division_name AS division_name,
    company_rollup.division_id AS division_id,
    source_sub_pod_id as sub_pod_id,
    sub_pod_name,
    FROM google.XP_DailyCurrentStats_F
  WHERE
    billing_category = 'Billable'
    AND service_region = 'Americas'
    AND service_channel = 'LCS'
    AND date >= '2024-01-01'
),

MetricDates AS
(
  SELECT
    MAX(IF(yt_brand_rxf_golden_rules_denominator > 0, date, NULL)) as max_date_yt_brand_rxf_golden_rules,
    MAX(IF(demand_gen_best_practices_denominator > 0, date, NULL)) as max_date_dg_bp,
    MAX(IF(dv360_media_unification_denominator > 0, date, NULL)) as max_date_dv360,
    MAX(IF(creative_optimization_denominator > 0, date, NULL)) as max_date_creative_optimization,
    MAX(IF(retail_demand_gen_best_practices_denominator > 0, date, NULL)) as max_date_retail_dg_bp,
    MAX(IF(data_strength_denominator > 0, date, NULL)) as max_date_data_strength,
    $BOQ($REPORTING_DATE) AS boq_date,
    $EOQ($REPORTING_DATE) AS eoq_date
  FROM pdw.prod.bfm.DailyCurrentAccountStats_DSF
  WHERE date >= $BOQ($REPORTING_DATE)
    AND service_region = 'Americas'
    AND service_channel = 'LCS'
    AND billing_Category = 'Billable'
),

BFM_Base AS
(
SELECT
  division_id,
  sub_pod_id,

  MD.max_date_yt_brand_rxf_golden_rules,
  MD.max_date_dg_bp,
  MD.max_date_dv360,
  MD.max_date_creative_optimization,
  MD.max_date_retail_dg_bp,
  MD.max_date_data_strength,
  MD.boq_date,
  MD.eoq_date,

//**********************************************************************************************************************************************************

--********************** 1. YT Brand RxF Golden Rules **********************

// YT Brand RxF Golden Rules
  SUM(IF(BFM.date= MD.boq_date,yt_brand_rxf_golden_rules_numerator,0)) AS yt_brand_rxf_golden_rules_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,yt_brand_rxf_golden_rules_denominator,0)) AS yt_brand_rxf_golden_rules_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_yt_brand_rxf_golden_rules,yt_brand_rxf_golden_rules_numerator,0)) AS yt_brand_rxf_golden_rules_numerator,
  SUM(IF(BFM.date= MD.max_date_yt_brand_rxf_golden_rules,yt_brand_rxf_golden_rules_denominator,0)) AS yt_brand_rxf_golden_rules_denominator,

//**********************************************************************************************************************************************************

-- ********************** 2. Demand Gen Best Practices ****************************
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_numerator,0)) AS demand_gen_best_practices_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_denominator,0)) AS demand_gen_best_practices_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_numerator,0)) AS demand_gen_best_practices_numerator,
  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_denominator,0)) AS demand_gen_best_practices_denominator,

// DG Best Practices - Audience
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_audience_numerator,0)) AS demand_gen_best_practices_audience_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_audience_numerator,0)) AS demand_gen_best_practices_audience_numerator,

// DG Best Practices - Budget
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_budget_numerator,0)) AS demand_gen_best_practices_budget_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_budget_numerator,0)) AS demand_gen_best_practices_budget_numerator,

// DG Best Practices - Creative
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_creative_numerator,0)) AS demand_gen_best_practices_creative_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_creative_numerator,0)) AS demand_gen_best_practices_creative_numerator,

// DG Best Practices - Data Strength Apps
  SUM(IF(BFM.date= MD.boq_date,demand_gen_best_practices_data_strength_apps_numerator,0)) AS demand_gen_best_practices_data_strength_apps_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_dg_bp,demand_gen_best_practices_data_strength_apps_numerator,0)) AS demand_gen_best_practices_data_strength_apps_numerator,

//**********************************************************************************************************************************************************

-- ********************** 3. DV360 Media Unification ****************************
  SUM(IF(BFM.date= MD.boq_date,dv360_media_unification_numerator,0)) AS dv360_media_unification_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,dv360_media_unification_denominator,0)) AS dv360_media_unification_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_dv360,dv360_media_unification_numerator,0)) AS dv360_media_unification_numerator,
  SUM(IF(BFM.date= MD.max_date_dv360,dv360_media_unification_denominator,0)) AS dv360_media_unification_denominator,


//***********************************************************************************************************************************************************

-- ********************** 4. Creative Optimization ****************************
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_numerator,0)) AS creative_optimization_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_denominator,0)) AS creative_optimization_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_numerator,0)) AS creative_optimization_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_denominator,0)) AS creative_optimization_denominator,


// Creative Optimization - Brand Auction Video
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_brand_auction_video_numerator,0)) AS creative_optimization_brand_auction_video_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_brand_auction_video_denominator,0)) AS creative_optimization_brand_auction_video_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_brand_auction_video_numerator,0)) AS creative_optimization_brand_auction_video_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_brand_auction_video_denominator,0)) AS creative_optimization_brand_auction_video_denominator,

// Creative Optimization - VRC
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_vrc_numerator,0)) AS creative_optimization_vrc_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_vrc_denominator,0)) AS creative_optimization_vrc_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_vrc_numerator,0)) AS creative_optimization_vrc_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_vrc_denominator,0)) AS creative_optimization_vrc_denominator,

// Creative Optimization - VVC
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_vvc_numerator,0)) AS creative_optimization_vvc_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_vvc_denominator,0)) AS creative_optimization_vvc_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_vvc_numerator,0)) AS creative_optimization_vvc_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_vvc_denominator,0)) AS creative_optimization_vvc_denominator,

// Creative Optimization - Demand Gen
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_demand_gen_numerator,0)) AS creative_optimization_demand_gen_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_demand_gen_denominator,0)) AS creative_optimization_demand_gen_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_demand_gen_numerator,0)) AS creative_optimization_demand_gen_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_demand_gen_denominator,0)) AS creative_optimization_demand_gen_denominator,

// Creative Optimization - Display (GDA)
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_gda_numerator,0)) AS creative_optimization_gda_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_gda_denominator,0)) AS creative_optimization_gda_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_gda_numerator,0)) AS creative_optimization_gda_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_gda_denominator,0)) AS creative_optimization_gda_denominator,

// Partnership Ads
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_partnership_ads_numerator,0)) AS creative_optimization_partnership_ads_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_partnership_ads_denominator,0)) AS creative_optimization_partnership_ads_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_partnership_ads_numerator,0)) AS creative_optimization_partnership_ads_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_partnership_ads_denominator,0)) AS creative_optimization_partnership_ads_denominator,

//Shorts Only
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_shorts_only_numerator,0)) AS creative_optimization_shorts_only_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_shorts_only_denominator,0)) AS creative_optimization_shorts_only_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_shorts_only_numerator,0)) AS creative_optimization_shorts_only_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_shorts_only_denominator,0)) AS creative_optimization_shorts_only_denominator,

//Asset Variety
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_asset_variety_numerator,0)) AS creative_optimization_asset_variety_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,creative_optimization_asset_variety_denominator,0)) AS creative_optimization_asset_variety_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_asset_variety_numerator,0)) AS creative_optimization_asset_variety_numerator,
  SUM(IF(BFM.date= MD.max_date_creative_optimization,creative_optimization_asset_variety_denominator,0)) AS creative_optimization_asset_variety_denominator,



-- ********************** 5. Retail Demand Gen Best Practices ****************************
  SUM(IF(BFM.date= MD.boq_date,retail_demand_gen_best_practices_numerator,0)) AS retail_demand_gen_best_practices_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,retail_demand_gen_best_practices_denominator,0)) AS retail_demand_gen_best_practices_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_retail_dg_bp,retail_demand_gen_best_practices_numerator,0)) AS retail_demand_gen_best_practices_numerator,
  SUM(IF(BFM.date= MD.max_date_retail_dg_bp,retail_demand_gen_best_practices_denominator,0)) AS retail_demand_gen_best_practices_denominator,

// DG Strength
  SUM(IF(BFM.date= MD.boq_date,retail_demand_gen_best_practices_dg_strength_numerator,0)) AS retail_demand_gen_best_practices_dg_strength_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_retail_dg_bp,retail_demand_gen_best_practices_dg_strength_numerator,0)) AS retail_demand_gen_best_practices_dg_strength_numerator,

// GMC
  SUM(IF(BFM.date= MD.boq_date,retail_demand_gen_best_practices_gmc_numerator,0)) AS retail_demand_gen_best_practices_gmc_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_retail_dg_bp,retail_demand_gen_best_practices_gmc_numerator,0)) AS retail_demand_gen_best_practices_gmc_numerator,

//**********************************************************************************************************************************************************
--1. ********************** Data Strength (P0) **********************

// Data Strength
  SUM(IF(BFM.date= MD.boq_date,data_strength_numerator,0)) AS data_strength_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,data_strength_denominator,0)) AS data_strength_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_numerator,0)) AS data_strength_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_denominator,0)) AS data_strength_denominator,


// Data Strength - GTG
  SUM(IF(BFM.date= MD.boq_date,data_strength_google_tag_gateway_numerator,0)) AS data_strength_google_tag_gateway_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,data_strength_google_tag_gateway_denominator,0)) AS data_strength_google_tag_gateway_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_google_tag_gateway_numerator,0)) AS data_strength_google_tag_gateway_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_google_tag_gateway_denominator,0)) AS data_strength_google_tag_gateway_denominator,

// Data Strength - EC for Web
  SUM(IF(BFM.date= MD.boq_date,data_strength_ecw_numerator,0)) AS data_strength_ecw_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,data_strength_ecw_denominator,0)) AS data_strength_ecw_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_ecw_numerator,0)) AS data_strength_ecw_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_ecw_denominator,0)) AS data_strength_ecw_denominator,

// Data Strength - Hybrid
  SUM(IF(BFM.date= MD.boq_date,data_strength_hybrid_numerator,0)) AS data_strength_hybrid_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,data_strength_hybrid_denominator,0)) AS data_strength_hybrid_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_hybrid_numerator,0)) AS data_strength_hybrid_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_hybrid_denominator,0)) AS data_strength_hybrid_denominator,

// Data Strength - Conversion Imports 3+ Identifiers
  SUM(IF(BFM.date= MD.boq_date,data_strength_conversions_import_with_identifiers_numerator,0)) AS data_strength_conversions_import_with_identifiers_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,data_strength_conversions_import_with_identifiers_denominator,0)) AS data_strength_conversions_import_with_identifiers_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_conversions_import_with_identifiers_numerator,0)) AS data_strength_conversions_import_with_identifiers_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_conversions_import_with_identifiers_denominator,0)) AS data_strength_conversions_import_with_identifiers_denominator,


FROM
  pdw.prod.bfm.DailyCurrentAccountStats_DSF AS BFM
  CROSS JOIN MetricDates AS MD

WHERE
  BFM.date >= MD.boq_date
  AND BFM.service_region = 'Americas'
  AND BFM.service_channel = 'LCS'
  AND BFM.billing_Category = 'Billable'

GROUP BY ALL
),

BFM_With_EOQ_Pacing AS
(
SELECT *,


// Brand RXF Golden Rules EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator) - SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator_boq, yt_brand_rxf_golden_rules_denominator_boq)),
DATE_DIFF(max_date_yt_brand_rxf_golden_rules ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_yt_brand_rxf_golden_rules, DAY))
+ SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator)
)
> 1
THEN 1 * yt_brand_rxf_golden_rules_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator) - SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator_boq, yt_brand_rxf_golden_rules_denominator_boq)),
DATE_DIFF(max_date_yt_brand_rxf_golden_rules, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_yt_brand_rxf_golden_rules, DAY))
+ SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator)
)
< 0
THEN 0 * yt_brand_rxf_golden_rules_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator) - SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator_boq, yt_brand_rxf_golden_rules_denominator_boq)),
DATE_DIFF(max_date_yt_brand_rxf_golden_rules, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_yt_brand_rxf_golden_rules, DAY))
+ SAFE_DIVIDE(yt_brand_rxf_golden_rules_numerator, yt_brand_rxf_golden_rules_denominator)
)
* yt_brand_rxf_golden_rules_denominator
END AS yt_brand_rxf_golden_rules_eoq_pacing_num,

//***********************************************************************************************************************************************************

// Demand Gen Best Practices EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator) - SAFE_DIVIDE(demand_gen_best_practices_numerator_boq, demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_dg_bp ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_dg_bp, DAY))
+ SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator)
)
> 1
THEN 1 * demand_gen_best_practices_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator) - SAFE_DIVIDE(demand_gen_best_practices_numerator_boq, demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_dg_bp, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_dg_bp, DAY))
+ SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator)
)
< 0
THEN 0 * demand_gen_best_practices_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator) - SAFE_DIVIDE(demand_gen_best_practices_numerator_boq, demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_dg_bp, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_dg_bp, DAY))
+ SAFE_DIVIDE(demand_gen_best_practices_numerator, demand_gen_best_practices_denominator)
)
* demand_gen_best_practices_denominator
END AS demand_gen_best_practices_eoq_pacing_num,

//***********************************************************************************************************************************************************

// DV360 Media Unification EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator) - SAFE_DIVIDE(dv360_media_unification_numerator_boq, dv360_media_unification_denominator_boq)),
DATE_DIFF(max_date_dv360 ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_dv360, DAY))
+ SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator)
)
> 1
THEN 1 * dv360_media_unification_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator) - SAFE_DIVIDE(dv360_media_unification_numerator_boq, dv360_media_unification_denominator_boq)),
DATE_DIFF(max_date_dv360, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_dv360, DAY))
+ SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator)
)
< 0
THEN 0 * dv360_media_unification_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator) - SAFE_DIVIDE(dv360_media_unification_numerator_boq, dv360_media_unification_denominator_boq)),
DATE_DIFF(max_date_dv360, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_dv360, DAY))
+ SAFE_DIVIDE(dv360_media_unification_numerator, dv360_media_unification_denominator)
)
* dv360_media_unification_denominator
END AS dv360_media_unification_eoq_pacing_num,

//***********************************************************************************************************************************************************
// Creative Optimization EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator) - SAFE_DIVIDE(creative_optimization_numerator_boq, creative_optimization_denominator_boq)),
DATE_DIFF(max_date_creative_optimization ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_creative_optimization, DAY))
+ SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator)
)
> 1
THEN 1 * creative_optimization_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator) - SAFE_DIVIDE(creative_optimization_numerator_boq, creative_optimization_denominator_boq)),
DATE_DIFF(max_date_creative_optimization, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_creative_optimization, DAY))
+ SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator)
)
< 0
THEN 0 * creative_optimization_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator) - SAFE_DIVIDE(creative_optimization_numerator_boq, creative_optimization_denominator_boq)),
DATE_DIFF(max_date_creative_optimization, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_creative_optimization, DAY))
+ SAFE_DIVIDE(creative_optimization_numerator, creative_optimization_denominator)
)
* creative_optimization_denominator
END AS creative_optimization_eoq_pacing_num,

//***********************************************************************************************************************************************************
// Retail Demand Gen Best Practices EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator) - SAFE_DIVIDE(retail_demand_gen_best_practices_numerator_boq, retail_demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_retail_dg_bp ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_retail_dg_bp, DAY))
+ SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator)
)
> 1
THEN 1 * retail_demand_gen_best_practices_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator) - SAFE_DIVIDE(retail_demand_gen_best_practices_numerator_boq, retail_demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_retail_dg_bp, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_retail_dg_bp, DAY))
+ SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator)
)
< 0
THEN 0 * retail_demand_gen_best_practices_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(
SAFE_DIVIDE((SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator) - SAFE_DIVIDE(retail_demand_gen_best_practices_numerator_boq, retail_demand_gen_best_practices_denominator_boq)),
DATE_DIFF(max_date_retail_dg_bp, boq_date, DAY))
* DATE_DIFF(eoq_date, max_date_retail_dg_bp, DAY))
+ SAFE_DIVIDE(retail_demand_gen_best_practices_numerator, retail_demand_gen_best_practices_denominator)
)
* retail_demand_gen_best_practices_denominator
END AS retail_demand_gen_best_practices_eoq_pacing_num,

 FROM BFM_Base
)

SELECT * FROM BFM_With_EOQ_Pacing;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
