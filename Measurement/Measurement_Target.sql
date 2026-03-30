DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.measurement_bfm_targets;

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

SUM(IF(bfm_name = 'data_strength',target_num,0.0)) AS data_strength_target_num,
SUM(IF(bfm_name = 'data_strength',target_den,0.0)) AS data_strength_target_den,
MAX(IF(bfm_name = 'data_strength',baseline_date,NULL)) AS data_strength_baseline_date,

SUM(IF(bfm_name = 'mmm_best_practices',target_num,0.0)) AS mmm_best_practices_target_num,
SUM(IF(bfm_name = 'mmm_best_practices',target_den,0.0)) AS mmm_best_practices_target_den,
MAX(IF(bfm_name = 'mmm_best_practices',baseline_date,NULL)) AS mmm_best_practices_baseline_date,

SUM(IF(bfm_name = 'incrementality',target_num,0.0)) AS incrementality_target_num,
SUM(IF(bfm_name = 'incrementality',target_den,0.0)) AS incrementality_target_den,
MAX(IF(bfm_name = 'incrementality',baseline_date,NULL)) AS incrementality_baseline_date

FROM
  emerson_team.division_level_bfm_adoption_2026q1 AS Targets

GROUP BY ALL
)

SELECT * FROM Targets_Base;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY;
