DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.search_parent_pipeline;


DEFINE MACRO MAX_AS MAX($1) AS $1;
DEFINE MACRO SUM_AS SUM($1) AS $1;

DEFINE MACRO QUERY 
WITH 
Customer_Dimensions AS
(
    SELECT
      CAST(parent_id AS INT64) AS parent_id,
      CAST(sub_pod_id AS STRING) AS sub_pod_id,
      CAST(NULL AS INT64) AS division_id,
      CAST(NULL AS STRING) AS division_name,
      'Unknown' AS front_end,
      'LCS' AS service_channel,
      FALSE AS is_css,
      ANY_VALUE(parent_name) AS parent_name,
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
    GROUP BY 1, 2
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

Pipeline_Base AS
(  
SELECT
      parent_id AS parent_id,
      source_sub_pod_id AS sub_pod_id,
      SUM(pipeline_outlook) AS pipeline_outlook,
      SUM(safe_revenue) AS secured_revenue,
      SUM(always_on_baseline) AS always_on_baseline,

-- Planned
SUM(total_tq_incremental_planned) AS incremental_planned,
SUM(total_tq_defense_planned) AS defense_planned,
SUM(total_tq_incremental_planned) + SUM(total_tq_defense_planned) AS total_planned,

-- Pitched+
(SUM(total_tq_incremental_pitched) + SUM(total_tq_defense_pitched) + SUM(total_tq_incremental_agreed) + SUM(total_tq_defense_agreed)) AS pitched_plus_num,

(SUM(total_tq_incremental_planned)
+ SUM(total_tq_incremental_pitched)
+ SUM(total_tq_incremental_agreed)
+ SUM(total_tq_defense_planned)
+ SUM(total_tq_defense_pitched)
+ SUM(total_tq_defense_agreed)
) AS pitched_plus_den,

      SUM(always_on_baseline)
      + SUM(total_tq_incremental_agreed)
      - SUM(total_tq_defense_planned)
      - SUM(total_tq_defense_pitched) AS adjusted_baseline_and_agreed,

-- Agreed+
(SUM(total_tq_incremental_agreed) + SUM(total_tq_defense_agreed)) AS agreed_plus_num,

(
SUM(total_tq_incremental_planned)
+ SUM(total_tq_incremental_pitched)
+ SUM(total_tq_incremental_agreed)
+ SUM(total_tq_defense_planned)
+ SUM(total_tq_defense_pitched)
+ SUM(total_tq_defense_agreed)
)
AS agreed_plus_den,      

    FROM onegtm.alcs_pipeline_dash_2025.pipeline_calculations.root.latest AS _t
    WHERE
      (quarter = "2026-Q1")
      //AND (product_area = "Search")
      AND is_search_plus = TRUE
      AND ADS_Revenue_Advertiser_Service_Channel ='LCS'
    GROUP BY ALL
    
  
)

SELECT 
D.*,

//Customer Dimensions
CD.division_id,
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

CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,

FROM   Pipeline_Base AS D

LEFT JOIN Customer_Dimensions AS CD
ON SAFE_CAST(D.parent_id AS STRING) = SAFE_CAST(CD.parent_id AS STRING)
AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(CD.sub_pod_id AS STRING)

LEFT OUTER JOIN finance_segmentation AS FS
ON SAFE_CAST(CD.parent_id AS STRING) = SAFE_CAST(FS.parent_id AS STRING)
AND SAFE_CAST($SECTOR_REGION AS STRING) = SAFE_CAST(FS.sector_region AS STRING)
;

CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
