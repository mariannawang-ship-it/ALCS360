DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.measurement_bfm;


DEFINE MACRO REPORTING_DATE DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY);

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
    MAX(IF(data_strength_denominator > 0, date, NULL)) as max_date_data_strength,
    MAX(IF(mmm_best_practices_denominator > 0, date, NULL)) as max_date_mmm_best_practices,
    MAX(IF(incrementality_denominator > 0, date, NULL)) as max_date_incrementality,
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
        MD.max_date_incrementality,
        MD.max_date_data_strength,
        MD.max_date_mmm_best_practices,


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


//**********************************************************************************************************************************************************

-- ********************** 2. MMM Best Practices (P1) ****************************
  SUM(IF(BFM.date= MD.boq_date,mmm_best_practices_numerator,0)) AS mmm_best_practices_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,mmm_best_practices_denominator,0)) AS mmm_best_practices_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_mmm_best_practices,mmm_best_practices_numerator,0)) AS mmm_best_practices_numerator,
  SUM(IF(BFM.date= MD.max_date_mmm_best_practices,mmm_best_practices_denominator,0)) AS mmm_best_practices_denominator,

// MMM Best Practices - Consult
  SUM(IF(BFM.date= MD.boq_date,mmm_best_practices_consult_numerator,0)) AS mmm_best_practices_consult_numerator_boq,

  SUM(IF(BFM.date= MD.max_date_mmm_best_practices,mmm_best_practices_consult_numerator,0)) AS mmm_best_practices_consult_numerator,


//**********************************************************************************************************************************************************

-- ********************** 3. Incrementality ****************************
  SUM(IF(BFM.date= MD.boq_date,incrementality_numerator,0)) AS incrementality_numerator_boq,
  SUM(IF(BFM.date= MD.boq_date,incrementality_denominator,0)) AS incrementality_denominator_boq,

  SUM(IF(BFM.date= MD.max_date_incrementality,incrementality_numerator,0)) AS incrementality_numerator,
  SUM(IF(BFM.date= MD.max_date_incrementality,incrementality_denominator,0)) AS incrementality_denominator,  

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

//***********************************************************************************************************************************************************

// MMM Best Practices EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator) - SAFE_DIVIDE(mmm_best_practices_numerator_boq, mmm_best_practices_denominator_boq)),
DATE_DIFF(max_date_mmm_best_practices ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_mmm_best_practices, DAY))
+ SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator)
)
> 1
THEN 1 * mmm_best_practices_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator) - SAFE_DIVIDE(mmm_best_practices_numerator_boq, mmm_best_practices_denominator_boq)),
DATE_DIFF(max_date_mmm_best_practices ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_mmm_best_practices, DAY))
+ SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator)
)
< 0
THEN 0 * mmm_best_practices_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator) - SAFE_DIVIDE(mmm_best_practices_numerator_boq, mmm_best_practices_denominator_boq)),
DATE_DIFF(max_date_mmm_best_practices ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_mmm_best_practices, DAY))
+ SAFE_DIVIDE(mmm_best_practices_numerator, mmm_best_practices_denominator)
)
* mmm_best_practices_denominator
END AS mmm_best_practices_eoq_pacing_num,

//***********************************************************************************************************************************************************

// Incrementality EOQ Pacing
CASE
-- If the eoq pacing is greater than 1, then the division is pacing to meet the target
WHEN
(
(SAFE_DIVIDE((SAFE_DIVIDE(incrementality_numerator, incrementality_denominator) - SAFE_DIVIDE(incrementality_numerator_boq, incrementality_denominator_boq)),
DATE_DIFF(max_date_incrementality ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_incrementality, DAY))
+ SAFE_DIVIDE(incrementality_numerator, incrementality_denominator)
)
> 1
THEN 1 * incrementality_denominator
WHEN
-- If the eoq pacing is less than 0, then the division is not pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(incrementality_numerator, incrementality_denominator) - SAFE_DIVIDE(incrementality_numerator_boq, incrementality_denominator_boq)),
DATE_DIFF(max_date_incrementality ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_incrementality, DAY))
+ SAFE_DIVIDE(incrementality_numerator, incrementality_denominator)
)
< 0
THEN 0 * incrementality_denominator
ELSE
-- If the eoq pacing is between 0 and 1, then the division is pacing to meet the target
(
(SAFE_DIVIDE((SAFE_DIVIDE(incrementality_numerator, incrementality_denominator) - SAFE_DIVIDE(incrementality_numerator_boq, incrementality_denominator_boq)),
DATE_DIFF(max_date_incrementality ,boq_date, DAY))  # latest update date for the given metric
* DATE_DIFF(eoq_date, max_date_incrementality, DAY))
+ SAFE_DIVIDE(incrementality_numerator, incrementality_denominator)
)
* incrementality_denominator
END AS incrementality_eoq_pacing_num,

 FROM BFM_Base
)


SELECT * FROM BFM_With_EOQ_Pacing;


CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
