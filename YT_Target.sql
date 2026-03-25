DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.yt_bfm_targets;


DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY);

DEFINE MACRO BOQ DATE_TRUNC($1,QUARTER);

DEFINE MACRO MAX_AS MAX($1) AS $1;

DEFINE MACRO QUERY
WITH
Targets_Base AS
(
SELECT
  division_id,
  sub_pod_id,

SUM(IF(bfm_name = 'yt_brand_rxf_golden_rules',target_num,0.0)) AS yt_brand_rxf_golden_rules_target_num,
SUM(IF(bfm_name = 'yt_brand_rxf_golden_rules',target_den,0.0)) AS yt_brand_rxf_golden_rules_target_den,
MAX(IF(bfm_name = 'yt_brand_rxf_golden_rules',baseline_date,NULL)) AS yt_brand_rxf_golden_rules_baseline_date,

SUM(IF(bfm_name = 'dv360_media_unification',target_num,0.0)) AS dv360_media_unification_target_num,
SUM(IF(bfm_name = 'dv360_media_unification',target_den,0.0)) AS dv360_media_unification_target_den,
MAX(IF(bfm_name = 'dv360_media_unification',baseline_date,NULL)) AS dv360_media_unification_baseline_date,

SUM(IF(bfm_name = 'retail_demand_gen_best_practices',target_num,0.0)) AS retail_demand_gen_best_practices_target_num,
SUM(IF(bfm_name = 'retail_demand_gen_best_practices',target_den,0.0)) AS retail_demand_gen_best_practices_target_den,
MAX(IF(bfm_name = 'retail_demand_gen_best_practices',baseline_date,NULL)) AS retail_demand_gen_best_practices_baseline_date,

SUM(IF(bfm_name = 'creative_optimization',target_num,0.0)) AS creative_optimization_target_num,
SUM(IF(bfm_name = 'creative_optimization',target_den,0.0)) AS creative_optimization_target_den,
MAX(IF(bfm_name = 'creative_optimization',baseline_date,NULL)) AS creative_optimization_baseline_date,

SUM(IF(bfm_name = 'demand_gen_best_practices',target_num,0.0)) AS demand_gen_best_practices_target_num,
SUM(IF(bfm_name = 'demand_gen_best_practices',target_den,0.0)) AS demand_gen_best_practices_target_den,
MAX(IF(bfm_name = 'demand_gen_best_practices',baseline_date,NULL)) AS demand_gen_best_practices_baseline_date

FROM
  emerson_team.division_level_bfm_adoption_2026q1 AS Targets

GROUP BY ALL
)

SELECT * FROM Targets_Base;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY;
