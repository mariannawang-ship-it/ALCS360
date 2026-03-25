DEFINE MACRO FULL_TABLE_NAME avgtm.play_reporting_2026.q1.yt_combined_table;

DEFINE MACRO MAX_AS MAX($1) AS $1;
DEFINE MACRO SUM_AS SUM($1) AS $1;

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
SELECT * FROM avgtm.play_reporting_2026.q1.yt_revenue
),

BFM AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.yt_bfm
),

BFM_Targets AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.yt_bfm_targets
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
//WHERE region = 'Americas'
//AND service_channel = 'LCS'
GROUP BY ALL
),

activation_map AS
(
  SELECT
    DISTINCT
    division_id,
    country_code,
    yt_tv_shift,
    yt_social_shift,
    yt_tv_social_shift,
    yt_create_demand,
    yt_dv3_media_unification,
    yt_dv3_performance,
    yt_dv3_commerce,
   FROM avgtm.play_reporting_2026.q1.activation_map
),

tv_social_spend AS
(
  SELECT * FROM avgtm.play_reporting_2026.q1.tv_social_addressable_spend
),

ALL_Dimensions AS (
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Revenue
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM_Targets
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM tv_social_spend
GROUP BY ALL
),

Combined_Base AS
(
SELECT
D.*,

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

  R.* EXCEPT (sub_pod_id, division_id),
  B.* EXCEPT (sub_pod_id, division_id),
  B_T.* EXCEPT (sub_pod_id, division_id),
  CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,
  TSS.* EXCEPT (sub_pod_id, division_id),

  CASE WHEN AM.yt_tv_shift = TRUE THEN TRUE ELSE FALSE END AS yt_tv_shift,
  CASE WHEN AM.yt_social_shift = TRUE THEN TRUE ELSE FALSE END AS yt_social_shift,
  CASE WHEN AM.yt_tv_social_shift = TRUE THEN TRUE ELSE FALSE END AS yt_tv_social_shift,
  CASE WHEN AM.yt_create_demand = TRUE THEN TRUE ELSE FALSE END AS yt_create_demand,
  CASE WHEN AM.yt_dv3_media_unification = TRUE THEN TRUE ELSE FALSE END AS yt_dv3_media_unification,
  CASE WHEN AM.yt_dv3_performance = TRUE THEN TRUE ELSE FALSE END AS yt_dv3_performance,
  CASE WHEN AM.yt_dv3_commerce = TRUE THEN TRUE ELSE FALSE END AS yt_dv3_commerce,

'Non Pipeline Row' AS row_type

FROM  ALL_Dimensions AS D

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

LEFT OUTER JOIN finance_segmentation AS FS
ON SAFE_CAST(CD.parent_id AS STRING) = SAFE_CAST(FS.parent_id AS STRING)
AND SAFE_CAST($SECTOR_REGION AS STRING) = SAFE_CAST(FS.sector_region AS STRING)

LEFT OUTER JOIN activation_map AS AM
ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(AM.division_id AS STRING)
AND SAFE_CAST(CD.service_country_code AS STRING) = SAFE_CAST(AM.country_code AS STRING)

LEFT OUTER JOIN tv_social_spend AS TSS
ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(TSS.division_id AS STRING)
AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(TSS.sub_pod_id AS STRING)
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
yt_plus_total_rev_qtd,
yt_plus_total_rev_qtd_ly,
yt_plus_total_rev_ytd,
yt_plus_total_rev_ytd_ly,
yt_plus_total_eoqx,
yt_plus_total_quota_fullq,
yt_plus_total_quota_qtd,
yt_plus_total_eoqx_surplus,
yt_secondary_rev_qtd,
yt_secondary_rev_qtd_ly,
yt_secondary_rev_ytd,
yt_secondary_rev_ytd_ly,
yt_secondary_eoqx,
yt_secondary_quota_fullq,
yt_secondary_quota_qtd,
yt_secondary_eoqx_surplus,
yt_brand_rev_qtd,
yt_brand_rev_qtd_ly,
yt_brand_rev_ytd,
yt_brand_rev_ytd_ly,
yt_brand_eoqx,
yt_brand_quota_fullq,
yt_brand_quota_qtd,
yt_brand_eoqx_surplus,
dg_rev_qtd,
dg_rev_qtd_ly,
dg_rev_ytd,
dg_rev_ytd_ly,
dg_eoqx,
dg_quota_fullq,
dg_quota_qtd,
dg_eoqx_surplus,
yt_dg_rev_qtd,
yt_dg_rev_qtd_ly,
yt_dg_rev_ytd,
yt_dg_rev_ytd_ly,
yt_dg_eoqx,
yt_dg_quota_fullq,
yt_dg_quota_qtd,
yt_dg_eoqx_surplus,
dva_dg_rev_qtd,
dva_dg_rev_qtd_ly,
dva_dg_rev_ytd,
dva_dg_rev_ytd_ly,
dva_dg_eoqx,
dva_dg_quota_fullq,
dva_dg_quota_qtd,
dva_dg_eoqx_surplus,
dva_video_rev_qtd,
dva_video_rev_qtd_ly,
dva_video_rev_ytd,
dva_video_rev_ytd_ly,
dva_video_eoqx,
dva_video_quota_fullq,
dva_video_quota_qtd,
dva_video_eoqx_surplus,
dva_standard_rev_qtd,
dva_standard_rev_qtd_ly,
dva_standard_rev_ytd,
dva_standard_rev_ytd_ly,
dva_standard_eoqx,
dva_standard_quota_fullq,
dva_standard_quota_qtd,
dva_standard_eoqx_surplus,
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
yt_apps_rev_qtd,
yt_apps_rev_qtd_ly,
yt_apps_rev_ytd,
yt_apps_rev_ytd_ly,
yt_apps_eoqx,
yt_apps_quota_fullq,
yt_apps_quota_qtd,
yt_apps_eoqx_surplus,
dva_apps_rev_qtd,
dva_apps_rev_qtd_ly,
dva_apps_rev_ytd,
dva_apps_rev_ytd_ly,
dva_apps_eoqx,
dva_apps_quota_fullq,
dva_apps_quota_qtd,
dva_apps_eoqx_surplus,
dv3_yt_plus_total_rev_qtd,
dv3_yt_plus_total_rev_qtd_ly,
dv3_yt_plus_total_rev_ytd,
dv3_yt_plus_total_rev_ytd_ly,
dv3_yt_plus_total_eoqx,
dv3_yt_plus_total_quota_fullq,
dv3_yt_plus_total_quota_qtd,
dv3_yt_plus_total_eoqx_surplus,

max_date_yt_brand_rxf_golden_rules,
max_date_dg_bp,
max_date_dv360,
max_date_creative_optimization,
max_date_retail_dg_bp,

yt_brand_rxf_golden_rules_numerator_boq,
yt_brand_rxf_golden_rules_denominator_boq,
yt_brand_rxf_golden_rules_numerator,
yt_brand_rxf_golden_rules_denominator,
yt_brand_rxf_golden_rules_eoq_pacing_num,
demand_gen_best_practices_numerator_boq,
demand_gen_best_practices_denominator_boq,
demand_gen_best_practices_numerator,
demand_gen_best_practices_denominator,
demand_gen_best_practices_eoq_pacing_num,
demand_gen_best_practices_audience_numerator_boq,
demand_gen_best_practices_audience_numerator,
demand_gen_best_practices_budget_numerator_boq,
demand_gen_best_practices_budget_numerator,
demand_gen_best_practices_creative_numerator_boq,
demand_gen_best_practices_creative_numerator,
demand_gen_best_practices_data_strength_apps_numerator_boq,
demand_gen_best_practices_data_strength_apps_numerator,
dv360_media_unification_numerator_boq,
dv360_media_unification_denominator_boq,
dv360_media_unification_numerator,
dv360_media_unification_denominator,
dv360_media_unification_eoq_pacing_num,
creative_optimization_numerator_boq,
creative_optimization_denominator_boq,
creative_optimization_numerator,
creative_optimization_denominator,
creative_optimization_eoq_pacing_num,
creative_optimization_brand_auction_video_numerator_boq,
creative_optimization_brand_auction_video_denominator_boq,
creative_optimization_brand_auction_video_numerator,
creative_optimization_brand_auction_video_denominator,
creative_optimization_vrc_numerator_boq,
creative_optimization_vrc_denominator_boq,
creative_optimization_vrc_numerator,
creative_optimization_vrc_denominator,
creative_optimization_vvc_numerator_boq,
creative_optimization_vvc_denominator_boq,
creative_optimization_vvc_numerator,
creative_optimization_vvc_denominator,
creative_optimization_demand_gen_numerator_boq,
creative_optimization_demand_gen_denominator_boq,
creative_optimization_demand_gen_numerator,
creative_optimization_demand_gen_denominator,
creative_optimization_gda_numerator_boq,
creative_optimization_gda_denominator_boq,
creative_optimization_gda_numerator,
creative_optimization_gda_denominator,
creative_optimization_partnership_ads_numerator_boq,
creative_optimization_partnership_ads_denominator_boq,
creative_optimization_partnership_ads_numerator,
creative_optimization_partnership_ads_denominator,
creative_optimization_shorts_only_numerator_boq,
creative_optimization_shorts_only_denominator_boq,
creative_optimization_shorts_only_numerator,
creative_optimization_shorts_only_denominator,
creative_optimization_asset_variety_numerator_boq,
creative_optimization_asset_variety_denominator_boq,
creative_optimization_asset_variety_numerator,
creative_optimization_asset_variety_denominator,
retail_demand_gen_best_practices_numerator_boq,
retail_demand_gen_best_practices_denominator_boq,
retail_demand_gen_best_practices_numerator,
retail_demand_gen_best_practices_denominator,
retail_demand_gen_best_practices_eoq_pacing_num,
retail_demand_gen_best_practices_dg_strength_numerator_boq,
retail_demand_gen_best_practices_dg_strength_numerator,
retail_demand_gen_best_practices_gmc_numerator_boq,
retail_demand_gen_best_practices_gmc_numerator,

//BFM Targets
yt_brand_rxf_golden_rules_target_num,
yt_brand_rxf_golden_rules_target_den,
yt_brand_rxf_golden_rules_baseline_date,
dv360_media_unification_target_num,
dv360_media_unification_target_den,
dv360_media_unification_baseline_date,
retail_demand_gen_best_practices_target_num,
retail_demand_gen_best_practices_target_den,
retail_demand_gen_best_practices_baseline_date,
creative_optimization_target_num,
creative_optimization_target_den,
creative_optimization_baseline_date,
demand_gen_best_practices_target_num,
demand_gen_best_practices_target_den,
demand_gen_best_practices_baseline_date,

whtt,
yt_tv_shift,
yt_social_shift,
yt_tv_social_shift,
yt_create_demand,
yt_dv3_media_unification,
yt_dv3_performance,
yt_dv3_commerce,
social_spend,
tv_spend,
row_type,

ARRAY_TO_STRING(
  ARRAY_CONCAT(
    IF(yt_tv_shift, ['TV Shift'], []),
    IF(yt_social_shift, ['Social Shift'], []),
    IF(yt_tv_social_shift, ['TV+ Social Shift'], []),
    IF(yt_create_demand, ['Create Demand'], []),
    IF(yt_dv3_media_unification, ['DV3 Media Unification'], []),
    IF(yt_dv3_performance, ['DV3 Performance'], []),
    IF(yt_dv3_commerce, ['DV3 Commerce'], [])
  ),
  ' / '
) AS yt_csb_group,

//Pipeline Metrics
NULL AS pipeline_outlook,
NULL AS secured_revenue,
NULL AS always_on_baseline,
NULL AS quota,
NULL AS incremental_planned,
NULL AS defense_planned,
NULL AS total_planned,
NULL AS pitched_plus_num,
NULL AS pitched_plus_den,
NULL AS agreed_plus_num,
NULL AS agreed_plus_den,
NULL AS adjusted_baseline_and_agreed,

NULL AS yt_secondary_pipeline_outlook,
NULL AS yt_secondary_quota,
NULL AS yt_secondary_secured_revenue,
NULL AS yt_secondary_always_on_baseline,
NULL AS yt_secondary_incremental_planned,
NULL AS yt_secondary_defense_planned,
NULL AS yt_secondary_total_planned,
NULL AS yt_secondary_pitched_plus_num,
NULL AS yt_secondary_pitched_plus_den,
NULL AS yt_secondary_agreed_plus_num,
NULL AS yt_secondary_agreed_plus_den,
NULL AS yt_secondary_adjusted_baseline_and_agreed,

NULL AS display_pipeline_outlook,
NULL AS display_quota,
NULL AS display_secured_revenue,
NULL AS display_always_on_baseline,
NULL AS display_incremental_planned,
NULL AS display_defense_planned,
NULL AS display_total_planned,
NULL AS display_pitched_plus_num,
NULL AS display_pitched_plus_den,
NULL AS display_agreed_plus_num,
NULL AS display_agreed_plus_den,
NULL AS display_adjusted_baseline_and_agreed


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
division_name,
parent_id,
division_id,
is_css,
sector_region,
sector_geo_order,

NULL AS yt_plus_total_rev_qtd,
NULL AS yt_plus_total_rev_qtd_ly,
NULL AS yt_plus_total_rev_ytd,
NULL AS yt_plus_total_rev_ytd_ly,
NULL AS yt_plus_total_eoqx,
NULL AS yt_plus_total_quota_fullq,
NULL AS yt_plus_total_quota_qtd,
NULL AS yt_plus_total_eoqx_surplus,
NULL AS yt_secondary_rev_qtd,
NULL AS yt_secondary_rev_qtd_ly,
NULL AS yt_secondary_rev_ytd,
NULL AS yt_secondary_rev_ytd_ly,
NULL AS yt_secondary_eoqx,
NULL AS yt_secondary_quota_fullq,
NULL AS yt_secondary_quota_qtd,
NULL AS yt_secondary_eoqx_surplus,
NULL AS yt_brand_rev_qtd,
NULL AS yt_brand_rev_qtd_ly,
NULL AS yt_brand_rev_ytd,
NULL AS yt_brand_rev_ytd_ly,
NULL AS yt_brand_eoqx,
NULL AS yt_brand_quota_fullq,
NULL AS yt_brand_quota_qtd,
NULL AS yt_brand_eoqx_surplus,
NULL AS dg_rev_qtd,
NULL AS dg_rev_qtd_ly,
NULL AS dg_rev_ytd,
NULL AS dg_rev_ytd_ly,
NULL AS dg_eoqx,
NULL AS dg_quota_fullq,
NULL AS dg_quota_qtd,
NULL AS dg_eoqx_surplus,
NULL AS yt_dg_rev_qtd,
NULL AS yt_dg_rev_qtd_ly,
NULL AS yt_dg_rev_ytd,
NULL AS yt_dg_rev_ytd_ly,
NULL AS yt_dg_eoqx,
NULL AS yt_dg_quota_fullq,
NULL AS yt_dg_quota_qtd,
NULL AS yt_dg_eoqx_surplus,
NULL AS dva_dg_rev_qtd,
NULL AS dva_dg_rev_qtd_ly,
NULL AS dva_dg_rev_ytd,
NULL AS dva_dg_rev_ytd_ly,
NULL AS dva_dg_eoqx,
NULL AS dva_dg_quota_fullq,
NULL AS dva_dg_quota_qtd,
NULL AS dva_dg_eoqx_surplus,
NULL AS dva_video_rev_qtd,
NULL AS dva_video_rev_qtd_ly,
NULL AS dva_video_rev_ytd,
NULL AS dva_video_rev_ytd_ly,
NULL AS dva_video_eoqx,
NULL AS dva_video_quota_fullq,
NULL AS dva_video_quota_qtd,
NULL AS dva_video_eoqx_surplus,
NULL AS dva_standard_rev_qtd,
NULL AS dva_standard_rev_qtd_ly,
NULL AS dva_standard_rev_ytd,
NULL AS dva_standard_rev_ytd_ly,
NULL AS dva_standard_eoqx,
NULL AS dva_standard_quota_fullq,
NULL AS dva_standard_quota_qtd,
NULL AS dva_standard_eoqx_surplus,
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
NULL AS yt_apps_rev_qtd,
NULL AS yt_apps_rev_qtd_ly,
NULL AS yt_apps_rev_ytd,
NULL AS yt_apps_rev_ytd_ly,
NULL AS yt_apps_eoqx,
NULL AS yt_apps_quota_fullq,
NULL AS yt_apps_quota_qtd,
NULL AS yt_apps_eoqx_surplus,
NULL AS dva_apps_rev_qtd,
NULL AS dva_apps_rev_qtd_ly,
NULL AS dva_apps_rev_ytd,
NULL AS dva_apps_rev_ytd_ly,
NULL AS dva_apps_eoqx,
NULL AS dva_apps_quota_fullq,
NULL AS dva_apps_quota_qtd,
NULL AS dva_apps_eoqx_surplus,
NULL AS dv3_yt_plus_total_rev_qtd,
NULL AS dv3_yt_plus_total_rev_qtd_ly,
NULL AS dv3_yt_plus_total_rev_ytd,
NULL AS dv3_yt_plus_total_rev_ytd_ly,
NULL AS dv3_yt_plus_total_eoqx,
NULL AS dv3_yt_plus_total_quota_fullq,
NULL AS dv3_yt_plus_total_quota_qtd,
NULL AS dv3_yt_plus_total_eoqx_surplus,

NULL AS max_date_yt_brand_rxf_golden_rules,
NULL AS max_date_dg_bp,
NULL AS max_date_dv360,
NULL AS max_date_creative_optimization,
NULL AS max_date_retail_dg_bp,

NULL AS yt_brand_rxf_golden_rules_numerator_boq,
NULL AS yt_brand_rxf_golden_rules_denominator_boq,
NULL AS yt_brand_rxf_golden_rules_numerator,
NULL AS yt_brand_rxf_golden_rules_denominator,
NULL AS yt_brand_rxf_golden_rules_eoq_pacing_num,
NULL AS demand_gen_best_practices_numerator_boq,
NULL AS demand_gen_best_practices_denominator_boq,
NULL AS demand_gen_best_practices_numerator,
NULL AS demand_gen_best_practices_denominator,
NULL AS demand_gen_best_practices_eoq_pacing_num,
NULL AS demand_gen_best_practices_audience_numerator_boq,
NULL AS demand_gen_best_practices_audience_numerator,
NULL AS demand_gen_best_practices_budget_numerator_boq,
NULL AS demand_gen_best_practices_budget_numerator,
NULL AS demand_gen_best_practices_creative_numerator_boq,
NULL AS demand_gen_best_practices_creative_numerator,
NULL AS demand_gen_best_practices_data_strength_apps_numerator_boq,
NULL AS demand_gen_best_practices_data_strength_apps_numerator,
NULL AS dv360_media_unification_numerator_boq,
NULL AS dv360_media_unification_denominator_boq,
NULL AS dv360_media_unification_numerator,
NULL AS dv360_media_unification_denominator,
NULL AS dv360_media_unification_eoq_pacing_num,
NULL AS creative_optimization_numerator_boq,
NULL AS creative_optimization_denominator_boq,
NULL AS creative_optimization_numerator,
NULL AS creative_optimization_denominator,
NULL AS creative_optimization_eoq_pacing_num,
NULL AS creative_optimization_brand_auction_video_numerator_boq,
NULL AS creative_optimization_brand_auction_video_denominator_boq,
NULL AS creative_optimization_brand_auction_video_numerator,
NULL AS creative_optimization_brand_auction_video_denominator,
NULL AS creative_optimization_vrc_numerator_boq,
NULL AS creative_optimization_vrc_denominator_boq,
NULL AS creative_optimization_vrc_numerator,
NULL AS creative_optimization_vrc_denominator,
NULL AS creative_optimization_vvc_numerator_boq,
NULL AS creative_optimization_vvc_denominator_boq,
NULL AS creative_optimization_vvc_numerator,
NULL AS creative_optimization_vvc_denominator,
NULL AS creative_optimization_demand_gen_numerator_boq,
NULL AS creative_optimization_demand_gen_denominator_boq,
NULL AS creative_optimization_demand_gen_numerator,
NULL AS creative_optimization_demand_gen_denominator,
NULL AS creative_optimization_gda_numerator_boq,
NULL AS creative_optimization_gda_denominator_boq,
NULL AS creative_optimization_gda_numerator,
NULL AS creative_optimization_gda_denominator,
NULL AS creative_optimization_partnership_ads_numerator_boq,
NULL AS creative_optimization_partnership_ads_denominator_boq,
NULL AS creative_optimization_partnership_ads_numerator,
NULL AS creative_optimization_partnership_ads_denominator,
NULL AS creative_optimization_shorts_only_numerator_boq,
NULL AS creative_optimization_shorts_only_denominator_boq,
NULL AS creative_optimization_shorts_only_numerator,
NULL AS creative_optimization_shorts_only_denominator,
NULL AS creative_optimization_asset_variety_numerator_boq,
NULL AS creative_optimization_asset_variety_denominator_boq,
NULL AS creative_optimization_asset_variety_numerator,
NULL AS creative_optimization_asset_variety_denominator,
NULL AS retail_demand_gen_best_practices_numerator_boq,
NULL AS retail_demand_gen_best_practices_denominator_boq,
NULL AS retail_demand_gen_best_practices_numerator,
NULL AS retail_demand_gen_best_practices_denominator,
NULL AS retail_demand_gen_best_practices_eoq_pacing_num,
NULL AS retail_demand_gen_best_practices_dg_strength_numerator_boq,
NULL AS retail_demand_gen_best_practices_dg_strength_numerator,
NULL AS retail_demand_gen_best_practices_gmc_numerator_boq,
NULL AS retail_demand_gen_best_practices_gmc_numerator,

NULL AS yt_brand_rxf_golden_rules_target_num,
NULL AS yt_brand_rxf_golden_rules_target_den,
NULL AS yt_brand_rxf_golden_rules_baseline_date,
NULL AS dv360_media_unification_target_num,
NULL AS dv360_media_unification_target_den,
NULL AS dv360_media_unification_baseline_date,
NULL AS retail_demand_gen_best_practices_target_num,
NULL AS retail_demand_gen_best_practices_target_den,
NULL AS retail_demand_gen_best_practices_baseline_date,
NULL AS creative_optimization_target_num,
NULL AS creative_optimization_target_den,
NULL AS creative_optimization_baseline_date,
NULL AS demand_gen_best_practices_target_num,
NULL AS demand_gen_best_practices_target_den,
NULL AS demand_gen_best_practices_baseline_date,

whtt,
NULL AS yt_tv_shift,
NULL AS yt_social_shift,
NULL AS yt_tv_social_shift,
NULL AS yt_create_demand,
NULL AS yt_dv3_media_unification,
NULL AS yt_dv3_performance,
NULL AS yt_dv3_commerce,
NULL AS social_spend,
NULL AS tv_spend,
'Pipeline Row' AS row_type,

NULL AS yt_csb_group,

//Pipeline Metrics
pipeline_outlook,
secured_revenue,
always_on_baseline,
quota,
incremental_planned,
defense_planned,
total_planned,
pitched_plus_num,
pitched_plus_den,
agreed_plus_num,
agreed_plus_den,
adjusted_baseline_and_agreed,

yt_secondary_pipeline_outlook,
yt_secondary_quota,
yt_secondary_secured_revenue,
yt_secondary_always_on_baseline,
yt_secondary_incremental_planned,
yt_secondary_defense_planned,
yt_secondary_total_planned,
yt_secondary_pitched_plus_num,
yt_secondary_pitched_plus_den,
yt_secondary_agreed_plus_num,
yt_secondary_agreed_plus_den,
yt_secondary_adjusted_baseline_and_agreed,

display_pipeline_outlook,
display_quota,
display_secured_revenue,
display_always_on_baseline,
display_incremental_planned,
display_defense_planned,
display_total_planned,
display_pitched_plus_num,
display_pitched_plus_den,
display_agreed_plus_num,
display_agreed_plus_den,
display_adjusted_baseline_and_agreed

FROM avgtm.play_reporting_2026.q1.yt_parent_pipeline AS PP;


DEFINE MACRO CNS_PATH
  /cns/iq-d/home/avgtm/avgtm.play_reporting_2026/q1/yt_combined_table;

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
