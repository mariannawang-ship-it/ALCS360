DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.apps_bfm;


DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY);

DEFINE MACRO BOQ DATE_TRUNC($1,QUARTER);
DEFINE MACRO EOQ DATE_ADD(DATE_TRUNC(DATE_ADD($1, INTERVAL 1 QUARTER), QUARTER), INTERVAL -1 DAY);

DEFINE MACRO DAYS_REMAINING_IN_QUARTER DATE_DIFF($EOQ(max_date),max_date,DAY);

DEFINE MACRO DAYS_QTD DATE_DIFF(max_date,$BOQ(max_date),DAY);

DEFINE MACRO MAX_AS MAX($1) AS $1;

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
    MAX(IF(apps_ios_must_win_denominator > 0, date, NULL)) as max_date_apps_ios_must_win,
    MAX(IF(web_to_app_connect_depth_denominator > 0, date, NULL)) as max_date_web_to_app_connect_depth,
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

  MD.max_date_apps_ios_must_win,
  MD.max_date_web_to_app_connect_depth,
  MD.boq_date,
  MD.eoq_date,


//**********************************************************************************************************************************************************  

// Apps iOS Must Win
  SUM(IF(BFM.date= MD.boq_date,apps_ios_must_win_numerator,0)) AS apps_ios_must_win_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,apps_ios_must_win_denominator,0)) AS apps_ios_must_win_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_apps_ios_must_win,apps_ios_must_win_numerator,0)) AS apps_ios_must_win_numerator,
  SUM(IF(BFM.date= MD.max_date_apps_ios_must_win,apps_ios_must_win_denominator,0)) AS apps_ios_must_win_denominator,


// Web to App Connect
  SUM(IF(BFM.date= MD.boq_date,web_to_app_connect_depth_numerator,0)) AS web_to_app_connect_depth_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,web_to_app_connect_depth_denominator,0)) AS web_to_app_connect_depth_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_web_to_app_connect_depth,web_to_app_connect_depth_numerator,0)) AS web_to_app_connect_depth_numerator,
  SUM(IF(BFM.date= MD.max_date_web_to_app_connect_depth,web_to_app_connect_depth_denominator,0)) AS web_to_app_connect_depth_denominator


FROM
  pdw.prod.bfm.DailyCurrentAccountStats_DSF AS BFM
CROSS JOIN MetricDates MD  

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


// apps_ios_must_win EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator) - SAFE_DIVIDE(apps_ios_must_win_numerator_boq, apps_ios_must_win_denominator_boq)),
DATE_DIFF(max_date_apps_ios_must_win ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_apps_ios_must_win, DAY))
+ SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator)
)
> 1
THEN 1 * apps_ios_must_win_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator) - SAFE_DIVIDE(apps_ios_must_win_numerator_boq, apps_ios_must_win_denominator_boq)),
DATE_DIFF(max_date_apps_ios_must_win ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_apps_ios_must_win, DAY))
+ SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator)
)
< 0
THEN 0 * apps_ios_must_win_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator) - SAFE_DIVIDE(apps_ios_must_win_numerator_boq, apps_ios_must_win_denominator_boq)),
DATE_DIFF(max_date_apps_ios_must_win ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_apps_ios_must_win, DAY))
+ SAFE_DIVIDE(apps_ios_must_win_numerator, apps_ios_must_win_denominator)
)
* apps_ios_must_win_denominator
END AS apps_ios_must_win_eoq_pacing_num,

//***********************************************************************************************************************************************************

// web_to_app_connect_depth EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator) - SAFE_DIVIDE(web_to_app_connect_depth_numerator_boq, web_to_app_connect_depth_denominator_boq)),
DATE_DIFF(max_date_web_to_app_connect_depth,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_web_to_app_connect_depth, DAY))
+ SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator)
)
> 1
THEN 1 * web_to_app_connect_depth_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator) - SAFE_DIVIDE(web_to_app_connect_depth_numerator_boq, web_to_app_connect_depth_denominator_boq)),
DATE_DIFF(max_date_web_to_app_connect_depth,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_web_to_app_connect_depth, DAY))
+ SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator)
)
< 0
THEN 0 * web_to_app_connect_depth_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator) - SAFE_DIVIDE(web_to_app_connect_depth_numerator_boq, web_to_app_connect_depth_denominator_boq)),
DATE_DIFF(max_date_web_to_app_connect_depth,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_web_to_app_connect_depth, DAY))
+ SAFE_DIVIDE(web_to_app_connect_depth_numerator, web_to_app_connect_depth_denominator)
)
* web_to_app_connect_depth_denominator
END AS web_to_app_connect_depth_eoq_pacing_num,

 FROM BFM_Base
)

SELECT * FROM BFM_With_EOQ_Pacing;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
