DEFINE MACRO FULL_TABLE_NAME avgtm.play_reporting_2026.q1.search_combined_table;

DEFINE MACRO MAX_AS MAX($1) AS $1;
DEFINE MACRO SUM_AS SUM($1) AS $1;

// Reporting date for Search Auction Metrics - lagging by 3 days
DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

DEFINE MACRO QTD
  date BETWEEN DATE_TRUNC($REPORTING_DATE, QUARTER) AND $REPORTING_DATE;

DEFINE MACRO QUERY 
WITH 
Customer_Dimensions AS
(
    SELECT
      CAST(parent_id AS INT64) AS parent_id,
      CAST(division_id AS INT64) AS division_id,
      CAST(sub_pod_id AS STRING) AS sub_pod_id,
      'Unknown' AS front_end,
      'LCS' AS service_channel,
      FALSE AS is_css,
      ANY_VALUE(parent_name) AS parent_name,
      ANY_VALUE(division_name) AS division_name,
      ANY_VALUE(sub_pod_name) AS sub_pod_name,
      ANY_VALUE(sector_name) AS sector_name,
      ANY_VALUE(sub_sector_name) AS sub_sector_name,
      ANY_VALUE(service_country_code) AS service_country_code

    FROM pdw.prod.sales.Customer_DSF
    WHERE
      service_region = 'Americas'
      AND service_channel = 'LCS'
      AND billing_category = 'Billable'
      AND front_end != 'DS'
    GROUP BY 1, 2, 3
),  
  
Revenue AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.search_revenue
),

BFM AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.search_bfm
),

BFM_Targets AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.search_bfm_targets
),

Auction_Metrics AS
(
SELECT
division_id,
sub_pod_id,

  SUM(prev_cost) AS search_rev_qtd_ly,
  SUM(latest_cost) AS search_rev_qtd,
  SUM(change_cost) AS search_rev_change_yoy,

  --Key Auction categories
  SUM(lvl0_contrib_page_ad_clicks_adj) AS page_ad_clicks_numerator,
  SUM(lvl1_contrib_click_share_adj + lvl1_contrib_adv_participation_adj) AS clickshare_and_coverage_comb_numerator,
  SUM(lvl1_contrib_cpc_adj) AS cpc_comb_numerator,

  --Detailed CPC Breakout (2nd-level)
  SUM(lvl2_contrib_budget) AS budget_contrib_sum,
  SUM(lvl2_contrib_target) + SUM(lvl2_contrib_value_per_click) AS target_comb_numerator,
  SUM(lvl2_contrib_auction_intensity) + SUM(lvl2_contrib_bidder_performance)AS bid_intensity_comb_numerator,
  SUM(lvl1_contrib_cpc_adj) - (SUM(lvl2_contrib_budget) + SUM(lvl2_contrib_target) + SUM(lvl2_contrib_value_per_click) + SUM(lvl2_contrib_auction_intensity)+ SUM(lvl2_contrib_bidder_performance)) AS non_stable_changes_numerator,
  
  --Detailed Click Share & Coverage Breakout (2nd-level)
  SUM(lvl1_contrib_click_share_adj) AS clickshare_indv_numerator,
  SUM(lvl1_contrib_adv_participation_adj) AS participation_indv_numerator,

  MAX(date) AS search_auction_metrics_baseline_date

FROM avi_storage.search.avi_v2_weekly_decomp_dsf AS _t
WHERE
$QTD
AND (service_region = "Americas")
AND service_channel = 'LCS'
GROUP BY ALL    
),

finance_segmentation AS
(
SELECT 
CAST(parent_id AS STRING) AS parent_id, 
CASE
    WHEN amg = 'US-CG&E' THEN 'US-CGE'
    WHEN amg = 'Brazil' THEN 'BR'
    WHEN amg = 'Canada' THEN 'CA'
    WHEN amg = 'SpLatam' THEN 'SpLatAm'
    WHEN amg = 'US-SDS' THEN 'US-TLS'
    ELSE amg
    END AS sector_region,
CASE 
     WHEN whtt IN ('4-Other', NULL) THEN '4-Other' ELSE whtt END AS whtt     
FROM
google.alcs_whtt_segmentation_2026_01_13
GROUP BY ALL
),

activation_map AS
(
  SELECT 
    DISTINCT
    division_id,
    country_code,
    search_retail,
    search_leadgen,
    search_core
   FROM avgtm.play_reporting_2026.q1.activation_map
),

ALL_Dimensions AS (
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Revenue
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM_Targets
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Auction_Metrics
GROUP BY ALL
),


Combined_Base AS
(
SELECT D.* ,

//Customer Dimensions
CD.parent_id,
CD.parent_name,
CD.division_name,
CD.sub_pod_name,
CD.sector_name,
CD.sub_sector_name,
CD.service_country_code,
CD.front_end,
CD.service_channel,
CD.is_css,

//Pulling Sector Region & Subsector Region from onegtm_macros()
$SECTOR_REGION AS sector_region,
$SUB_SECTOR_REGION AS sub_sector_region,
$SECTOR_REGION_ORDER() AS sector_geo_order,

  //PT.* EXCEPT (sub_pod_id, division_id),
  R.* EXCEPT (sub_pod_id, division_id),
  B.* EXCEPT (sub_pod_id, division_id),
  B_T.* EXCEPT (sub_pod_id, division_id),
  AU.* EXCEPT (sub_pod_id, division_id),
  CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,

  CASE WHEN AM.search_retail = TRUE THEN TRUE ELSE FALSE END AS search_retail,
  CASE WHEN AM.search_leadgen = TRUE THEN TRUE ELSE FALSE END AS search_leadgen,
  CASE WHEN AM.search_core = TRUE THEN TRUE ELSE FALSE END AS search_core,

  'Non Pipeline Row' AS row_type


FROM  All_Dimensions AS D

  
  LEFT JOIN Customer_Dimensions AS CD
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(CD.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(CD.sub_pod_id AS STRING)

  LEFT OUTER JOIN Revenue AS R
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(R.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(R.sub_pod_id AS STRING)

  LEFT OUTER JOIN BFM AS B
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(B.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(B.sub_pod_id AS STRING)

  LEFT OUTER JOIN BFM_Targets AS B_T
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(B_T.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(B_T.sub_pod_id AS STRING)

  LEFT OUTER JOIN Auction_Metrics AS AU
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(AU.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(AU.sub_pod_id AS STRING)

  LEFT OUTER JOIN finance_segmentation AS FS
  ON SAFE_CAST(CD.parent_id AS STRING) = SAFE_CAST(FS.parent_id AS STRING)
  AND SAFE_CAST($SECTOR_REGION AS STRING) = SAFE_CAST(FS.sector_region AS STRING)

  LEFT OUTER JOIN activation_map AS AM
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(AM.division_id AS STRING)
  AND SAFE_CAST(CD.service_country_code AS STRING) = SAFE_CAST(AM.country_code AS STRING)

)


SELECT
front_end,
sub_sector_region,
service_channel,
service_country_code,
sector_name,
sub_sector_name,
sub_pod_name,
sub_pod_id,
parent_name,
division_name,
parent_id,
division_id,
is_css,
sector_region,
sector_geo_order,

search_total_rev_qtd,
search_total_rev_qtd_ly,
search_total_rev_ytd,
search_total_rev_ytd_ly,
search_total_eoqx,
search_total_quota_fullq,
search_total_quota_qtd,
search_total_eoqx_surplus,
search_text_rev_qtd,
search_text_rev_qtd_ly,
search_text_rev_ytd,
search_text_rev_ytd_ly,
search_text_eoqx,
search_text_quota_fullq,
search_text_quota_qtd,
search_text_eoqx_surplus,
search_pmax_rev_qtd,
search_pmax_rev_qtd_ly,
search_pmax_rev_ytd,
search_pmax_rev_ytd_ly,
search_pmax_eoqx,
search_pmax_quota_fullq,
search_pmax_quota_qtd,
search_pmax_eoqx_surplus,
search_app_rev_qtd,
search_app_rev_qtd_ly,
search_app_rev_ytd,
search_app_rev_ytd_ly,
search_app_eoqx,
search_app_quota_fullq,
search_app_quota_qtd,
search_app_eoqx_surplus,
search_shopping_rev_qtd,
search_shopping_rev_qtd_ly,
search_shopping_rev_ytd,
search_shopping_rev_ytd_ly,
search_shopping_eoqx,
search_shopping_quota_fullq,
search_shopping_quota_qtd,
search_shopping_eoqx_surplus,
search_others_rev_qtd,
search_others_rev_qtd_ly,
search_others_rev_ytd,
search_others_rev_ytd_ly,
search_others_eoqx,
search_others_quota_fullq,
search_others_quota_qtd,
search_others_eoqx_surplus,

yt_pmax_rev_qtd,
yt_pmax_rev_qtd_ly,
yt_pmax_rev_ytd,
yt_pmax_rev_ytd_ly,
yt_pmax_eoqx,
yt_pmax_quota_fullq,
yt_pmax_quota_qtd,
yt_pmax_eoqx_surplus,
dva_pmax_rev_qtd,
dva_pmax_rev_qtd_ly,
dva_pmax_rev_ytd,
dva_pmax_rev_ytd_ly,
dva_pmax_eoqx,
dva_pmax_quota_fullq,
dva_pmax_quota_qtd,
dva_pmax_eoqx_surplus,


search_clicks_qtd,
search_clicks_qtd_ly,
search_clicks_ytd,
search_clicks_ytd_ly,
search_impressions_qtd,
search_impressions_qtd_ly,
search_impressions_ytd,
search_impressions_ytd_ly,

//Search Auction Metrics from go/avi
search_rev_qtd,
search_rev_change_yoy,
search_rev_qtd_ly,
search_auction_metrics_baseline_date,

page_ad_clicks_numerator,
clickshare_and_coverage_comb_numerator,
cpc_comb_numerator,

  --Detailed CPC Breakout (2nd-level)
budget_contrib_sum,
target_comb_numerator,
bid_intensity_comb_numerator,
non_stable_changes_numerator,

  --Detailed Click Share & Coverage Breakout (2nd-level)
clickshare_indv_numerator,
participation_indv_numerator,


search_pmax_ai_readiness_numerator_boq,
search_pmax_ai_readiness_denominator_boq,
search_pmax_ai_readiness_numerator,
search_pmax_ai_readiness_denominator,
search_pmax_ai_readiness_eoq_pacing_num,

search_pmax_ai_readiness_ai_max_numerator_boq,
search_pmax_ai_readiness_ai_max_denominator_boq,
search_pmax_ai_readiness_ai_max_numerator,
search_pmax_ai_readiness_ai_max_denominator,
search_pmax_ai_readiness_pmax_search_numerator_boq,
search_pmax_ai_readiness_pmax_search_denominator_boq,
search_pmax_ai_readiness_pmax_search_numerator,
search_pmax_ai_readiness_pmax_search_denominator,
search_pmax_ai_readiness_ai_max_targeting_numerator_boq,
search_pmax_ai_readiness_ai_max_tc_numerator_boq,
search_pmax_ai_readiness_ai_max_fue_numerator_boq,
search_pmax_ai_readiness_ai_max_targeting_numerator,
search_pmax_ai_readiness_ai_max_tc_numerator,
search_pmax_ai_readiness_ai_max_fue_numerator,
search_pmax_ai_readiness_pmax_search_targeting_numerator_boq,
search_pmax_ai_readiness_pmax_search_tc_numerator_boq,
search_pmax_ai_readiness_pmax_search_fue_numerator_boq,
search_pmax_ai_readiness_pmax_search_targeting_numerator,
search_pmax_ai_readiness_pmax_search_tc_numerator,
search_pmax_ai_readiness_pmax_search_fue_numerator,

search_pmax_budget_target_sufficiency_numerator_boq,
search_pmax_budget_target_sufficiency_denominator_boq,
search_pmax_budget_target_sufficiency_numerator,
search_pmax_budget_target_sufficiency_denominator,
search_pmax_budget_target_sufficiency_eoq_pacing_num,

search_pmax_budget_target_sufficiency_budgets_denominator_boq,
search_pmax_budget_target_sufficiency_budgets_denominator,
search_pmax_budget_target_sufficiency_targets_denominator_boq,
search_pmax_budget_target_sufficiency_targets_denominator,
search_pmax_budget_target_sufficiency_pmax_numerator_boq,
search_pmax_budget_target_sufficiency_pmax_denominator_boq,
search_pmax_budget_target_sufficiency_pmax_numerator,
search_pmax_budget_target_sufficiency_pmax_denominator,
search_pmax_budget_target_sufficiency_pmax_budgets_denominator_boq,
search_pmax_budget_target_sufficiency_pmax_budgets_denominator,
search_pmax_budget_target_sufficiency_pmax_targets_denominator_boq,
search_pmax_budget_target_sufficiency_pmax_targets_denominator,
search_pmax_budget_target_sufficiency_search_numerator_boq,
search_pmax_budget_target_sufficiency_search_denominator_boq,
search_pmax_budget_target_sufficiency_search_numerator,
search_pmax_budget_target_sufficiency_search_denominator,
search_pmax_budget_target_sufficiency_search_budgets_denominator_boq,
search_pmax_budget_target_sufficiency_search_budgets_denominator,
search_pmax_budget_target_sufficiency_search_targets_denominator_boq,
search_pmax_budget_target_sufficiency_search_targets_denominator,
search_pmax_budget_target_sufficiency_search_shopping_numerator_boq,
search_pmax_budget_target_sufficiency_search_shopping_denominator_boq,
search_pmax_budget_target_sufficiency_search_shopping_numerator,
search_pmax_budget_target_sufficiency_search_shopping_denominator,
search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator_boq,
search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator,
search_pmax_budget_target_sufficiency_search_shopping_targets_denominator_boq,
search_pmax_budget_target_sufficiency_search_shopping_targets_denominator,
search_pmax_budget_target_sufficiency_shopping_numerator_boq,
search_pmax_budget_target_sufficiency_shopping_denominator_boq,
search_pmax_budget_target_sufficiency_shopping_numerator,
search_pmax_budget_target_sufficiency_shopping_denominator,
search_pmax_budget_target_sufficiency_shopping_budgets_denominator_boq,
search_pmax_budget_target_sufficiency_shopping_budgets_denominator,
search_pmax_budget_target_sufficiency_shopping_targets_denominator_boq,
search_pmax_budget_target_sufficiency_shopping_targets_denominator,
search_pmax_ai_bidding_numerator_boq,
search_pmax_ai_bidding_denominator_boq,
search_pmax_ai_bidding_numerator,
search_pmax_ai_bidding_denominator,
search_pmax_ai_bidding_eoq_pacing_num,
search_pmax_ai_bidding_vbb_numerator,
search_pmax_ai_bidding_vbb_numerator_boq,
search_pmax_ai_bidding_advanced_value_enhancer_numerator,
search_pmax_ai_bidding_advanced_value_enhancer_numerator_boq,
sa360_ai_bidding_numerator_boq,
sa360_ai_bidding_denominator_boq,
sa360_ai_bidding_numerator,
sa360_ai_bidding_denominator,
sa360_ai_bidding_eoq_pacing_num,
data_strength_numerator_boq,
data_strength_denominator_boq,
data_strength_numerator,
data_strength_denominator,
data_strength_eoq_pacing_num,
sa360_enterprise_bidding_numerator_boq,
sa360_enterprise_bidding_denominator_boq,
sa360_enterprise_bidding_numerator,
sa360_enterprise_bidding_denominator,
sa360_enterprise_bidding_eoq_pacing_num,

//BFM Targets
sa360_enterprise_bidding_target_num,
sa360_enterprise_bidding_target_den,
sa360_enterprise_bidding_baseline_date,
search_pmax_ai_readiness_target_num,
search_pmax_ai_readiness_target_den,
search_pmax_ai_readiness_baseline_date,
search_pmax_budget_target_sufficiency_target_num,
search_pmax_budget_target_sufficiency_target_den,
search_pmax_budget_target_sufficiency_baseline_date,
data_strength_target_num,
data_strength_target_den,
data_strength_baseline_date,

whtt,
search_retail,
search_leadgen,
search_core,

row_type,

//Pipeline Metrics
NULL AS pipeline_outlook,
NULL AS secured_revenue,
NULL AS always_on_baseline,
NULL AS incremental_planned,
NULL AS defense_planned,
NULL AS total_planned,
NULL AS pitched_plus_num,
NULL AS pitched_plus_den,
NULL AS agreed_plus_num,
NULL AS agreed_plus_den,
NULL AS adjusted_baseline_and_agreed,

FROM Combined_Base AS CB
UNION ALL

SELECT
front_end,
sub_sector_region,
service_channel,
service_country_code,
sector_name,
sub_sector_name,
sub_pod_name,
sub_pod_id,
parent_name,
NULL AS division_name,
parent_id,
NULL AS division_id,
is_css,
sector_region,
sector_geo_order,

NULL AS search_total_rev_qtd,
NULL AS search_total_rev_qtd_ly,
NULL AS search_total_rev_ytd,
NULL AS search_total_rev_ytd_ly,
NULL AS search_total_eoqx,
NULL AS search_total_quota_fullq,
NULL AS search_total_quota_qtd,
NULL AS search_total_eoqx_surplus,
NULL AS search_text_rev_qtd,
NULL AS search_text_rev_qtd_ly,
NULL AS search_text_rev_ytd,
NULL AS search_text_rev_ytd_ly,
NULL AS search_text_eoqx,
NULL AS search_text_quota_fullq,
NULL AS search_text_quota_qtd,
NULL AS search_text_eoqx_surplus,
NULL AS search_pmax_rev_qtd,
NULL AS search_pmax_rev_qtd_ly,
NULL AS search_pmax_rev_ytd,
NULL AS search_pmax_rev_ytd_ly,
NULL AS search_pmax_eoqx,
NULL AS search_pmax_quota_fullq,
NULL AS search_pmax_quota_qtd,
NULL AS search_pmax_eoqx_surplus,
NULL AS search_app_rev_qtd,
NULL AS search_app_rev_qtd_ly,
NULL AS search_app_rev_ytd,
NULL AS search_app_rev_ytd_ly,
NULL AS search_app_eoqx,
NULL AS search_app_quota_fullq,
NULL AS search_app_quota_qtd,
NULL AS search_app_eoqx_surplus,
NULL AS search_shopping_rev_qtd,
NULL AS search_shopping_rev_qtd_ly,
NULL AS search_shopping_rev_ytd,
NULL AS search_shopping_rev_ytd_ly,
NULL AS search_shopping_eoqx,
NULL AS search_shopping_quota_fullq,
NULL AS search_shopping_quota_qtd,
NULL AS search_shopping_eoqx_surplus,
NULL AS search_others_rev_qtd,
NULL AS search_others_rev_qtd_ly,
NULL AS search_others_rev_ytd,
NULL AS search_others_rev_ytd_ly,
NULL AS search_others_eoqx,
NULL AS search_others_quota_fullq,
NULL AS search_others_quota_qtd,
NULL AS search_others_eoqx_surplus,

NULL AS yt_pmax_rev_qtd,
NULL AS yt_pmax_rev_qtd_ly,
NULL AS yt_pmax_rev_ytd,
NULL AS yt_pmax_rev_ytd_ly,
NULL AS yt_pmax_eoqx,
NULL AS yt_pmax_quota_fullq,
NULL AS yt_pmax_quota_qtd,
NULL AS yt_pmax_eoqx_surplus,
NULL AS dva_pmax_rev_qtd,
NULL AS dva_pmax_rev_qtd_ly,
NULL AS dva_pmax_rev_ytd,
NULL AS dva_pmax_rev_ytd_ly,
NULL AS dva_pmax_eoqx,
NULL AS dva_pmax_quota_fullq,
NULL AS dva_pmax_quota_qtd,
NULL AS dva_pmax_eoqx_surplus,

NULL AS search_clicks_qtd,
NULL AS search_clicks_qtd_ly,
NULL AS search_clicks_ytd,
NULL AS search_clicks_ytd_ly,
NULL AS search_impressions_qtd,
NULL AS search_impressions_qtd_ly,
NULL AS search_impressions_ytd,
NULL AS search_impressions_ytd_ly,

NULL AS search_rev_qtd,
NULL AS search_rev_change_yoy,
NULL AS search_rev_qtd_ly,
NULL AS search_auction_metrics_baseline_date,

NULL AS page_ad_clicks_numerator,
NULL AS clickshare_and_coverage_comb_numerator,
NULL AS cpc_comb_numerator,
NULL AS budget_contrib_sum,
NULL AS target_comb_numerator,
NULL AS bid_intensity_comb_numerator,
NULL AS non_stable_changes_numerator,
NULL AS clickshare_indv_numerator,
NULL AS participation_indv_numerator,

NULL AS search_pmax_ai_readiness_numerator_boq,
NULL AS search_pmax_ai_readiness_denominator_boq,
NULL AS search_pmax_ai_readiness_numerator,
NULL AS search_pmax_ai_readiness_denominator,
NULL AS search_pmax_ai_readiness_eoq_pacing_num,

NULL AS search_pmax_ai_readiness_ai_max_numerator_boq,
NULL AS search_pmax_ai_readiness_ai_max_denominator_boq,
NULL AS search_pmax_ai_readiness_ai_max_numerator,
NULL AS search_pmax_ai_readiness_ai_max_denominator,
NULL AS search_pmax_ai_readiness_pmax_search_numerator_boq,
NULL AS search_pmax_ai_readiness_pmax_search_denominator_boq,
NULL AS search_pmax_ai_readiness_pmax_search_numerator,
NULL AS search_pmax_ai_readiness_pmax_search_denominator,
NULL AS search_pmax_ai_readiness_ai_max_targeting_numerator_boq,
NULL AS search_pmax_ai_readiness_ai_max_tc_numerator_boq,
NULL AS search_pmax_ai_readiness_ai_max_fue_numerator_boq,
NULL AS search_pmax_ai_readiness_ai_max_targeting_numerator,
NULL AS search_pmax_ai_readiness_ai_max_tc_numerator,
NULL AS search_pmax_ai_readiness_ai_max_fue_numerator,
NULL AS search_pmax_ai_readiness_pmax_search_targeting_numerator_boq,
NULL AS search_pmax_ai_readiness_pmax_search_tc_numerator_boq,
NULL AS search_pmax_ai_readiness_pmax_search_fue_numerator_boq,
NULL AS search_pmax_ai_readiness_pmax_search_targeting_numerator,
NULL AS search_pmax_ai_readiness_pmax_search_tc_numerator,
NULL AS search_pmax_ai_readiness_pmax_search_fue_numerator,
NULL AS search_pmax_budget_target_sufficiency_numerator_boq,
NULL AS search_pmax_budget_target_sufficiency_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_numerator,
NULL AS search_pmax_budget_target_sufficiency_denominator,
NULL AS search_pmax_budget_target_sufficiency_eoq_pacing_num,

NULL AS search_pmax_budget_target_sufficiency_budgets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_budgets_denominator,
NULL AS search_pmax_budget_target_sufficiency_targets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_targets_denominator,
NULL AS search_pmax_budget_target_sufficiency_pmax_numerator_boq,
NULL AS search_pmax_budget_target_sufficiency_pmax_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_pmax_numerator,
NULL AS search_pmax_budget_target_sufficiency_pmax_denominator,
NULL AS search_pmax_budget_target_sufficiency_pmax_budgets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_pmax_budgets_denominator,
NULL AS search_pmax_budget_target_sufficiency_pmax_targets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_pmax_targets_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_numerator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_numerator,
NULL AS search_pmax_budget_target_sufficiency_search_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_budgets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_budgets_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_targets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_targets_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_numerator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_numerator,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_targets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_search_shopping_targets_denominator,
NULL AS search_pmax_budget_target_sufficiency_shopping_numerator_boq,
NULL AS search_pmax_budget_target_sufficiency_shopping_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_shopping_numerator,
NULL AS search_pmax_budget_target_sufficiency_shopping_denominator,
NULL AS search_pmax_budget_target_sufficiency_shopping_budgets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_shopping_budgets_denominator,
NULL AS search_pmax_budget_target_sufficiency_shopping_targets_denominator_boq,
NULL AS search_pmax_budget_target_sufficiency_shopping_targets_denominator,
NULL AS search_pmax_ai_bidding_numerator_boq,
NULL AS search_pmax_ai_bidding_denominator_boq,
NULL AS search_pmax_ai_bidding_numerator,
NULL AS search_pmax_ai_bidding_denominator,
NULL AS search_pmax_ai_bidding_eoq_pacing_num,

NULL AS search_pmax_ai_bidding_vbb_numerator,
NULL AS search_pmax_ai_bidding_vbb_numerator_boq,
NULL AS search_pmax_ai_bidding_advanced_value_enhancer_numerator,
NULL AS search_pmax_ai_bidding_advanced_value_enhancer_numerator_boq,
NULL AS sa360_ai_bidding_numerator_boq,
NULL AS sa360_ai_bidding_denominator_boq,
NULL AS sa360_ai_bidding_numerator,
NULL AS sa360_ai_bidding_denominator,
NULL AS sa360_ai_bidding_eoq_pacing_num,

NULL AS data_strength_numerator_boq,
NULL AS data_strength_denominator_boq,
NULL AS data_strength_numerator,
NULL AS data_strength_denominator,
NULL AS data_strength_eoq_pacing_num,

NULL AS sa360_enterprise_bidding_numerator_boq,
NULL AS sa360_enterprise_bidding_denominator_boq,
NULL AS sa360_enterprise_bidding_numerator,
NULL AS sa360_enterprise_bidding_denominator,
NULL AS sa360_enterprise_bidding_eoq_pacing_num,

NULL AS sa360_enterprise_bidding_target_num,
NULL AS sa360_enterprise_bidding_target_den,
NULL AS sa360_enterprise_bidding_baseline_date,
NULL AS search_pmax_ai_readiness_target_num,
NULL AS search_pmax_ai_readiness_target_den,
NULL AS search_pmax_ai_readiness_baseline_date,
NULL AS search_pmax_budget_target_sufficiency_target_num,
NULL AS search_pmax_budget_target_sufficiency_target_den,
NULL AS search_pmax_budget_target_sufficiency_baseline_date,
NULL AS data_strength_target_num,
NULL AS data_strength_target_den,
NULL AS data_strength_baseline_date,

whtt,
NULL AS search_retail,
NULL AS search_leadgen,
NULL AS search_core,

'Pipeline Row' AS row_type,

//Pipeline Metrics
pipeline_outlook,
secured_revenue,
always_on_baseline,
incremental_planned,
defense_planned,
total_planned,
pitched_plus_num,
pitched_plus_den,
agreed_plus_num,
agreed_plus_den,
adjusted_baseline_and_agreed,

FROM avgtm.play_reporting_2026.q1.search_parent_pipeline AS PP;


DEFINE MACRO CNS_PATH
  /cns/iq-d/home/avgtm/avgtm.play_reporting_2026/q1/search_combined_table;

EXPORT DATA
  OPTIONS (
    format = 'capacitor',
    path = '$CNS_PATH/${YYYY}/${MM}/${DD}/files/data',
    owner = 'avgtm',
    overwrite_directory = TRUE)
AS
$QUERY;

CREATE OR REPLACE EXTERNAL TABLE $FULL_TABLE_NAME
  OPTIONS (
    format = 'ds_proto',
    ds_proto = """
      replica {
        capacitor { path:'$CNS_PATH/\${YYYY}/\${MM}/\${DD}/files/data*'}
      }
      data_owner:'mdb/avgtm'
      custom_suffix { 
        name: 'latest' 
        value { year: '${YYYY}' month: '${MM}' day:'${DD}' } 
        }
      """);
