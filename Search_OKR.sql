DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.search_bfm;


DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);

DEFINE MACRO BOQ DATE_TRUNC($1,QUARTER);

DEFINE MACRO EOQ DATE_ADD(DATE_TRUNC(DATE_ADD($1, INTERVAL 1 QUARTER), QUARTER), INTERVAL -1 DAY);

DEFINE MACRO DAYS_REMAINING_IN_QUARTER DATE_DIFF($EOQ(max_date),max_date,DAY);

DEFINE MACRO DAYS_QTD DATE_DIFF(max_date,$BOQ(max_date),DAY);

DEFINE MACRO MAX_AS MAX($1) AS $1;

DEFINE MACRO QUERY
WITH
MetricDates AS
(
  SELECT
    MAX(IF(search_pmax_ai_readiness_denominator > 0, date, NULL)) as max_date_search_pmax_ai_readiness,
    MAX(IF(search_pmax_budget_target_sufficiency_denominator > 0, date, NULL)) as max_date_search_pmax_budget_target_sufficiency,
    MAX(IF(search_pmax_ai_bidding_denominator > 0, date, NULL)) as max_date_search_pmax_ai_bidding,
    MAX(IF(sa360_ai_bidding_denominator > 0, date, NULL)) as max_date_sa360_ai_bidding,
    MAX(IF(data_strength_denominator > 0, date, NULL)) as max_date_data_strength,
    MAX(IF(sa360_enterprise_bidding_denominator > 0, date, NULL)) as max_date_sa360_enterprise_bidding,
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


  MD.boq_date,
  MD.eoq_date,
  MD.max_date_search_pmax_ai_readiness,
  MD.max_date_search_pmax_budget_target_sufficiency,
  MD.max_date_search_pmax_ai_bidding,
  MD.max_date_sa360_ai_bidding,
  MD.max_date_data_strength,
  MD.max_date_sa360_enterprise_bidding,

//**********************************************************************************************************************************************************  

--1. ********************** AI Readiness Metrics (P0) **********************

// Search PMax AI Readiness
  SUM(IF(BFM.date= MD.boq_date ,search_pmax_ai_readiness_numerator,0)) AS search_pmax_ai_readiness_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_readiness_denominator,0)) AS search_pmax_ai_readiness_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_numerator,0)) AS search_pmax_ai_readiness_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_denominator,0)) AS search_pmax_ai_readiness_denominator,


// Search PMax AI Readiness AI Max
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_readiness_ai_max_numerator,0)) AS search_pmax_ai_readiness_ai_max_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_readiness_ai_max_denominator,0)) AS search_pmax_ai_readiness_ai_max_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_ai_max_numerator,0)) AS search_pmax_ai_readiness_ai_max_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_ai_max_denominator,0)) AS search_pmax_ai_readiness_ai_max_denominator,


// Search PMax AI Readiness PMax Search
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_pmax_search_numerator,0)) AS search_pmax_ai_readiness_pmax_search_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_pmax_search_denominator,0)) AS search_pmax_ai_readiness_pmax_search_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_pmax_search_numerator,0)) AS search_pmax_ai_readiness_pmax_search_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_pmax_search_denominator,0)) AS search_pmax_ai_readiness_pmax_search_denominator,


// **************************** Search PMax AI Readiness AI Max Sub Metrics
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_ai_max_targeting_numerator,0)) AS search_pmax_ai_readiness_ai_max_targeting_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_ai_max_tc_numerator,0)) AS search_pmax_ai_readiness_ai_max_tc_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_ai_max_fue_numerator,0)) AS search_pmax_ai_readiness_ai_max_fue_numerator_boq,


  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_ai_max_targeting_numerator,0)) AS search_pmax_ai_readiness_ai_max_targeting_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_ai_max_tc_numerator,0)) AS search_pmax_ai_readiness_ai_max_tc_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_ai_max_fue_numerator,0)) AS search_pmax_ai_readiness_ai_max_fue_numerator,


// **************************** Search PMax AI Readiness PMax Search Sub Metrics
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_pmax_search_targeting_numerator,0)) AS search_pmax_ai_readiness_pmax_search_targeting_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_pmax_search_tc_numerator,0)) AS search_pmax_ai_readiness_pmax_search_tc_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,search_pmax_ai_readiness_pmax_search_fue_numerator,0)) AS search_pmax_ai_readiness_pmax_search_fue_numerator_boq,


  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_pmax_search_targeting_numerator,0)) AS search_pmax_ai_readiness_pmax_search_targeting_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_pmax_search_tc_numerator,0)) AS search_pmax_ai_readiness_pmax_search_tc_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_readiness,search_pmax_ai_readiness_pmax_search_fue_numerator,0)) AS search_pmax_ai_readiness_pmax_search_fue_numerator,

//**********************************************************************************************************************************************************

-- ********************** 2. Budget & Target Sufficiency Metrics (P0) ****************************
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_numerator,0)) AS search_pmax_budget_target_sufficiency_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_denominator,0)) AS search_pmax_budget_target_sufficiency_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_numerator,0)) AS search_pmax_budget_target_sufficiency_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_denominator,0)) AS search_pmax_budget_target_sufficiency_denominator,

// Budget Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_budgets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_budgets_denominator,

// Target Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_targets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_targets_denominator,


// **************************** A.PMax Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_pmax_numerator,0)) AS search_pmax_budget_target_sufficiency_pmax_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_pmax_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_pmax_numerator,0)) AS search_pmax_budget_target_sufficiency_pmax_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_pmax_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_denominator,

//PMax Sufficiency Sub Metrics

//PMax Budget Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_pmax_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_budgets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_pmax_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_budgets_denominator,

//PMax Target Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_pmax_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_targets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_pmax_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_pmax_targets_denominator,



// **************************** B.Search Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_numerator,0)) AS search_pmax_budget_target_sufficiency_search_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_denominator,0)) AS search_pmax_budget_target_sufficiency_search_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_numerator,0)) AS search_pmax_budget_target_sufficiency_search_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_denominator,0)) AS search_pmax_budget_target_sufficiency_search_denominator,

//Search Sufficiency Sub Metrics

//Search Budget Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_budgets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_budgets_denominator,

//Search Target Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_targets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_targets_denominator,



// **************************** C.Search + Shopping Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_shopping_numerator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_shopping_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_shopping_numerator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_shopping_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_denominator,

//Search + Shopping Sufficiency Sub Metrics

//Search + Shopping Budget Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_budgets_denominator,

//Search + Shopping Target Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_search_shopping_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_targets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_search_shopping_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_search_shopping_targets_denominator,



// **************************** D.Shopping Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_shopping_numerator,0)) AS search_pmax_budget_target_sufficiency_shopping_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_shopping_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_shopping_numerator,0)) AS search_pmax_budget_target_sufficiency_shopping_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_shopping_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_denominator,

//Shopping Sufficiency Sub Metrics

//Shopping Budget Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_shopping_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_budgets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_shopping_budgets_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_budgets_denominator,

//Shopping Target Sufficiency
  SUM(IF(BFM.date=MD.boq_date,search_pmax_budget_target_sufficiency_shopping_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_targets_denominator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_budget_target_sufficiency,search_pmax_budget_target_sufficiency_shopping_targets_denominator,0)) AS search_pmax_budget_target_sufficiency_shopping_targets_denominator,

//******************************************************************* P1 Metrics  *****************************************************************************

-- ********************** 3. Search PMax AI Bidding (P1) ****************************
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_bidding_numerator,0)) AS search_pmax_ai_bidding_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_bidding_denominator,0)) AS search_pmax_ai_bidding_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_bidding,search_pmax_ai_bidding_numerator,0)) AS search_pmax_ai_bidding_numerator,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_bidding,search_pmax_ai_bidding_denominator,0)) AS search_pmax_ai_bidding_denominator,


//Value Enhancer
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_bidding_advanced_value_enhancer_numerator,0)) AS search_pmax_ai_bidding_advanced_value_enhancer_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_bidding,search_pmax_ai_bidding_advanced_value_enhancer_numerator,0)) AS search_pmax_ai_bidding_advanced_value_enhancer_numerator,

//VBB
  SUM(IF(BFM.date=MD.boq_date,search_pmax_ai_bidding_vbb_numerator,0)) AS search_pmax_ai_bidding_vbb_numerator_boq,
  SUM(IF(BFM.date= MD.max_date_search_pmax_ai_bidding,search_pmax_ai_bidding_vbb_numerator,0)) AS search_pmax_ai_bidding_vbb_numerator,



-- ********************** 4. SA360 Enterprise Bidding (P1) ****************************
  SUM(IF(BFM.date=MD.boq_date,sa360_ai_bidding_numerator,0)) AS sa360_ai_bidding_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,sa360_ai_bidding_denominator,0)) AS sa360_ai_bidding_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_sa360_ai_bidding,sa360_ai_bidding_numerator,0)) AS sa360_ai_bidding_numerator,
  SUM(IF(BFM.date= MD.max_date_sa360_ai_bidding,sa360_ai_bidding_denominator,0)) AS sa360_ai_bidding_denominator,

-- ********************** 5. Data Strength (P1) ****************************
  SUM(IF(BFM.date=MD.boq_date,data_strength_numerator,0)) AS data_strength_numerator_boq,
  SUM(IF(BFM.date=MD.boq_date,data_strength_denominator,0)) AS data_strength_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_numerator,0)) AS data_strength_numerator,
  SUM(IF(BFM.date= MD.max_date_data_strength,data_strength_denominator,0)) AS data_strength_denominator,


-- SA 360 Enterprise Bidding
  SUM(IF(BFM.date= MD.boq_date,sa360_enterprise_bidding_numerator,0)) AS sa360_enterprise_bidding_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,sa360_enterprise_bidding_denominator,0)) AS sa360_enterprise_bidding_denominator_boq,
  
  SUM(IF(BFM.date= MD.max_date_sa360_enterprise_bidding,sa360_enterprise_bidding_numerator,0)) AS sa360_enterprise_bidding_numerator,
  SUM(IF(BFM.date= MD.max_date_sa360_enterprise_bidding,sa360_enterprise_bidding_denominator,0)) AS sa360_enterprise_bidding_denominator,

  

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


// Search PMax AI Readiness EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator) - SAFE_DIVIDE(search_pmax_ai_readiness_numerator_boq, search_pmax_ai_readiness_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_readiness ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_readiness, DAY))
+ SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator)
)
> 1
THEN 1 * search_pmax_ai_readiness_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator) - SAFE_DIVIDE(search_pmax_ai_readiness_numerator_boq, search_pmax_ai_readiness_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_readiness ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_readiness, DAY))
+ SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator)
)
< 0
THEN 0 * search_pmax_ai_readiness_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator) - SAFE_DIVIDE(search_pmax_ai_readiness_numerator_boq, search_pmax_ai_readiness_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_readiness ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_readiness, DAY))
+ SAFE_DIVIDE(search_pmax_ai_readiness_numerator, search_pmax_ai_readiness_denominator)
)
* search_pmax_ai_readiness_denominator
END AS search_pmax_ai_readiness_eoq_pacing_num,

//***********************************************************************************************************************************************************

// Budget & Target Sufficiency EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator) - SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator_boq, search_pmax_budget_target_sufficiency_denominator_boq)),
DATE_DIFF(max_date_search_pmax_budget_target_sufficiency ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_budget_target_sufficiency, DAY))
+ SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator)
)
> 1
THEN 1 * search_pmax_budget_target_sufficiency_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator) - SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator_boq, search_pmax_budget_target_sufficiency_denominator_boq)),
DATE_DIFF(max_date_search_pmax_budget_target_sufficiency ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_budget_target_sufficiency, DAY))
+ SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator)
)
< 0
THEN 0 * search_pmax_budget_target_sufficiency_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator) - SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator_boq, search_pmax_budget_target_sufficiency_denominator_boq)),
DATE_DIFF(max_date_search_pmax_budget_target_sufficiency ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_budget_target_sufficiency, DAY))
+ SAFE_DIVIDE(search_pmax_budget_target_sufficiency_numerator, search_pmax_budget_target_sufficiency_denominator)
)
* search_pmax_budget_target_sufficiency_denominator
END AS search_pmax_budget_target_sufficiency_eoq_pacing_num,

//***********************************************************************************************************************************************************

// Search PMax AI Bidding EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator) - SAFE_DIVIDE(search_pmax_ai_bidding_numerator_boq, search_pmax_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_bidding, DAY))
+ SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator)
)
> 1
THEN 1 * search_pmax_ai_bidding_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator) - SAFE_DIVIDE(search_pmax_ai_bidding_numerator_boq, search_pmax_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_bidding, DAY))
+ SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator)
)
< 0
THEN 0 * search_pmax_ai_bidding_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator) - SAFE_DIVIDE(search_pmax_ai_bidding_numerator_boq, search_pmax_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_search_pmax_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_search_pmax_ai_bidding, DAY))
+ SAFE_DIVIDE(search_pmax_ai_bidding_numerator, search_pmax_ai_bidding_denominator)
)
* search_pmax_ai_bidding_denominator
END AS search_pmax_ai_bidding_eoq_pacing_num,

//***********************************************************************************************************************************************************
// SA 360AI Bidding EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator) - SAFE_DIVIDE(sa360_ai_bidding_numerator_boq, sa360_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_ai_bidding, DAY))
+ SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator)
)
> 1
THEN 1 * sa360_ai_bidding_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator) - SAFE_DIVIDE(sa360_ai_bidding_numerator_boq, sa360_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_ai_bidding, DAY))
+ SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator)
)
< 0
THEN 0 * sa360_ai_bidding_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator) - SAFE_DIVIDE(sa360_ai_bidding_numerator_boq, sa360_ai_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_ai_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_ai_bidding, DAY))
+ SAFE_DIVIDE(sa360_ai_bidding_numerator, sa360_ai_bidding_denominator)
)
* sa360_ai_bidding_denominator
END AS sa360_ai_bidding_eoq_pacing_num,

//***********************************************************************************************************************************************************
// SA360 Enterprise Bidding EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator) - SAFE_DIVIDE(sa360_enterprise_bidding_numerator_boq, sa360_enterprise_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_enterprise_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_enterprise_bidding, DAY))
+ SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator)
)
> 1
THEN 1 * sa360_enterprise_bidding_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator) - SAFE_DIVIDE(sa360_enterprise_bidding_numerator_boq, sa360_enterprise_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_enterprise_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_enterprise_bidding, DAY))
+ SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator)
)
< 0
THEN 0 * sa360_enterprise_bidding_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator) - SAFE_DIVIDE(sa360_enterprise_bidding_numerator_boq, sa360_enterprise_bidding_denominator_boq)),
DATE_DIFF(max_date_sa360_enterprise_bidding ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_sa360_enterprise_bidding, DAY))
+ SAFE_DIVIDE(sa360_enterprise_bidding_numerator, sa360_enterprise_bidding_denominator)
)
* sa360_enterprise_bidding_denominator
END AS sa360_enterprise_bidding_eoq_pacing_num,

//***********************************************************************************************************************************************************
// Data Strength EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(data_strength_numerator, data_strength_denominator) - SAFE_DIVIDE(data_strength_numerator_boq, data_strength_denominator_boq)),
DATE_DIFF(max_date_data_strength ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_data_strength, DAY))
+ SAFE_DIVIDE(data_strength_numerator, data_strength_denominator)
)
> 1
THEN 1 * data_strength_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(data_strength_numerator, data_strength_denominator) - SAFE_DIVIDE(data_strength_numerator_boq, data_strength_denominator_boq)),
DATE_DIFF(max_date_data_strength ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_data_strength, DAY))
+ SAFE_DIVIDE(data_strength_numerator, data_strength_denominator)
)
< 0
THEN 0 * data_strength_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(data_strength_numerator, data_strength_denominator) - SAFE_DIVIDE(data_strength_numerator_boq, data_strength_denominator_boq)),
DATE_DIFF(max_date_data_strength ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_data_strength, DAY))
+ SAFE_DIVIDE(data_strength_numerator, data_strength_denominator)
)
* data_strength_denominator
END AS data_strength_eoq_pacing_num,

 FROM BFM_Base
)

SELECT * FROM BFM_With_EOQ_Pacing;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
