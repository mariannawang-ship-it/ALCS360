DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.search_bfm_targets;


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

SUM(IF(bfm_name = 'sa360_enterprise_bidding',target_num,0.0)) AS sa360_enterprise_bidding_target_num,
SUM(IF(bfm_name = 'sa360_enterprise_bidding',target_den,0.0)) AS sa360_enterprise_bidding_target_den,
MAX(IF(bfm_name = 'sa360_enterprise_bidding',baseline_date,NULL)) AS sa360_enterprise_bidding_baseline_date,

SUM(IF(bfm_name = 'search_pmax_ai_readiness',target_num,0.0)) AS search_pmax_ai_readiness_target_num,
SUM(IF(bfm_name = 'search_pmax_ai_readiness',target_den,0.0)) AS search_pmax_ai_readiness_target_den,
MAX(IF(bfm_name = 'search_pmax_ai_readiness',baseline_date,NULL)) AS search_pmax_ai_readiness_baseline_date,

SUM(IF(bfm_name = 'search_pmax_budget_target_sufficiency',target_num,0.0)) AS search_pmax_budget_target_sufficiency_target_num,
SUM(IF(bfm_name = 'search_pmax_budget_target_sufficiency',target_den,0.0)) AS search_pmax_budget_target_sufficiency_target_den,
MAX(IF(bfm_name = 'search_pmax_budget_target_sufficiency',baseline_date,NULL)) AS search_pmax_budget_target_sufficiency_baseline_date,

SUM(IF(bfm_name = 'data_strength',target_num,0.0)) AS data_strength_target_num,
SUM(IF(bfm_name = 'data_strength',target_den,0.0)) AS data_strength_target_den,
MAX(IF(bfm_name = 'data_strength',baseline_date,NULL)) AS data_strength_baseline_date,

FROM
  emerson_team.division_level_bfm_adoption_2026q1 AS Targets

GROUP BY ALL
)

SELECT * FROM Targets_Base;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY;
