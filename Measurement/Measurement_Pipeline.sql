DEFINE MACRO FULL_TABLE_NAME
  avgtm.play_reporting_2026.q1.measurement_pipeline;

DEFINE MACRO QS
  '2026-Q1';

DEFINE MACRO OFFERINGS_LIST
  'D.0095.01',
  'E.0092.01',
  'D.0101.01',
  'E.0098.01',
  'D.0007.01',
  'E.0007.01',
  'O.0009.01',
  'D.0008.01',
  'E.0008.01',
  'D.0092.01',
  'E.0089.01',
  'D.0045.01',
  'E.0042.01',
  'D.0030.01',
  'E.0030.01',
  'D.0070.01',
  'E.0067.01',
  'D.0073.01',
  'E.0070.01',
  'D.0054.01',
  'E.0051.01'
  ;
  

DEFINE MACRO PRODUCT_ACTION_LIST
"google tag gatewayadopt",
"google tag gatewayexpand",
"first party data in data manageradopt",
"first party data in data managerexpand",
"enhanced conversionsadopt",
"enhanced conversionsexpand",
"enhanced conversionsoptimize",
"enhanced conversions for leadsadopt",
"enhanced conversions for leadsexpand",
"mmm (marketing mix models)adopt",
"mmm (marketing mix models)expand",
"meridianadopt",
"meridianexpand",
"conversion liftexpand",
"conversion liftadopt",
"brand liftadopt",
"brand liftexpand",
"search liftexpand",
"search liftadopt",
"conversion lift based on geographyadopt",
"conversion lift based on geographyexpand"
;


DEFINE MACRO DS_PRODUCT_ACTION_LIST
"google tag gatewayadopt",
"google tag gatewayexpand",
"first party data in data manageradopt",
"first party data in data managerexpand",
"enhanced conversionsadopt",
"enhanced conversionsexpand",
"enhanced conversionsoptimize",
"enhanced conversions for leadsadopt",
"enhanced conversions for leadsexpand";


DEFINE MACRO MMM_PRODUCT_ACTION_LIST
"mmm (marketing mix models)adopt",
"mmm (marketing mix models)expand",
"meridianadopt",
"meridianexpand";

DEFINE MACRO INCREMENTALITY_PRODUCT_ACTION_LIST
"conversion liftexpand",
"conversion liftadopt",
"brand liftadopt",
"brand liftexpand",
"search liftexpand",
"search liftadopt",
"conversion lift based on geographyadopt",
"conversion lift based on geographyexpand";

DEFINE MACRO SUM_AS
  SUM($1) AS $1;

DEFINE MACRO IMPACT_VALUE
SUM(IF(qs.quarter_string=$QS,qs.impact.$1,0)) AS $1;

DEFINE MACRO IMPACT_VALUE_MQ
SUM(qs.impact.$1);

DEFINE MACRO QUERY
  (
    SELECT
      SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id,
      division_id,
      opportunity_id,
      opportunity_name,
      offering_type,
      offering_stage,
      offering_id AS offering_id,
      product_name AS offering_name,
      offering_intent_name AS intent_name,
      CONCAT(product_name, offering_intent_name) AS product_action,
      CASE WHEN campaign_id IS NULL THEN "-" ELSE CAST(campaign_id AS STRING) END AS campaign_id,
      'All' AS source,
      SAFE_CAST(qs.quarter_string AS STRING) AS quarter_string,

      $IMPACT_VALUE(impact_available),
      $IMPACT_VALUE(impact_planned),
      $IMPACT_VALUE(impact_accepted),
      $IMPACT_VALUE(impact_pitched),
      $IMPACT_VALUE(impact_agreed),
      $IMPACT_VALUE(impact_implemented),
      $IMPACT_VALUE(impact_lost),
      $IMPACT_VALUE(impact_abandoned),
      $IMPACT_VALUE(impact_dismissed)
    FROM
      ropps_analytics.pipeline p
    JOIN UNNEST(p.quarter_split) qs
    

      WHERE
         (
        qs.quarter_string >= $QS --in-quarter 
        OR ropps_cohort_quarter = $QS
        )
        AND (
        created_date <='2026-01-01' -- deployment date
        OR qs.quarter_string = $QS
        )
      AND region = 'Americas'
      AND pod_name LIKE '%LCS%'
      /*remove abandoned with older ropps_cohort_quartes*/
      AND IF(
        ropps_cohort_quarter >= $QS,
        TRUE,
        (
          IFNULL(CAST(lost_date AS DATE), CURRENT_DATE()) >= DATE_TRUNC(CURRENT_DATE, QUARTER)
          AND (offering_stage != 'ABANDONED')))

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
   
);


DEFINE MACRO TABLE_QUERY
SELECT
  A.sub_pod_id,
  A.division_id,
  A.opportunity_id,
  CONCAT('[', A.opportunity_name, '](https://sales.connect.corp.google.com/opportunities/', opportunity_id, '){target="_blank"}') AS opportunity_link,
  A.opportunity_name,
  A.offering_type,
  A.offering_stage,
  A.offering_id,
  A.offering_name,
  A.intent_name,
  A.product_action,
  A.campaign_id,
  source,
  quarter_string,

  CASE WHEN A.campaign_id IN ('D.0095.01','E.0092.01','D.0101.01','E.0098.01','D.0007.01','E.0007.01','O.0009.01','D.0008.01','E.0008.01') THEN 'Data Strength'
  WHEN LOWER(A.product_action) IN ($DS_PRODUCT_ACTION_LIST) THEN 'Data Strength'
  WHEN A.campaign_id IN ('D.0092.01','E.0089.01','D.0045.01','E.0042.01') THEN 'MMM'
  WHEN LOWER(A.product_action) IN ($MMM_PRODUCT_ACTION_LIST) THEN 'MMM'
  WHEN A.campaign_id IN ('D.0030.01','E.0030.01','D.0070.01','E.0067.01','D.0073.01','E.0070.01','D.0054.01','E.0051.01') THEN 'Incrementality'
  WHEN LOWER(A.product_action) IN ($INCREMENTALITY_PRODUCT_ACTION_LIST) THEN 'Incrementality'
  WHEN (LOWER(A.opportunity_name) LIKE '%value delivered%') THEN 'Value Delivered'
  ELSE 'Other' 
  END AS offering_type_group,

  $SUM_AS(impact_available),
  $SUM_AS(impact_planned),
  $SUM_AS(impact_pitched),
  $SUM_AS(impact_agreed),
  $SUM_AS(impact_implemented),
  $SUM_AS(impact_lost),
  $SUM_AS(impact_dismissed),
  $SUM_AS(impact_abandoned)
FROM
  $QUERY AS A
  WHERE 
   // A.impact_available > 0 AND
    (A.campaign_id IN ($OFFERINGS_LIST) OR LOWER(A.opportunity_name) LIKE '%value delivered%' 
    OR LOWER(A.product_action) IN ($PRODUCT_ACTION_LIST)
    )
GROUP BY ALL
;

CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $TABLE_QUERY;
