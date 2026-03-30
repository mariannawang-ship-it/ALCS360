DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.yt_parent_pipeline;


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
//WHERE region = 'Americas'
//AND service_channel = 'LCS'
GROUP BY ALL
),

Pipeline_Base AS
(
SELECT
      parent_id AS parent_id,
      source_sub_pod_id AS sub_pod_id,
      SUM(IF(product_area = 'YouTube', pipeline_outlook, 0.0)) AS pipeline_outlook,
      SUM(IF(product_area = 'YouTube',safe_revenue, 0.0)) AS secured_revenue,
      SUM(IF(product_area = 'YouTube',always_on_baseline, 0.0)) AS always_on_baseline,
      SUM(IF(product_area = 'YouTube',total_full_qtr_quota, 0.0)) AS quota,

-- Planned
SUM(IF(product_area = 'YouTube',total_tq_incremental_planned, 0.0)) AS incremental_planned,
SUM(IF(product_area = 'YouTube',total_tq_defense_planned, 0.0)) AS defense_planned,
(SUM(IF(product_area = 'YouTube',total_tq_incremental_planned, 0.0)) + SUM(IF(product_area = 'YouTube',total_tq_defense_planned, 0.0))) AS total_planned,

-- Pitched+
(SUM(IF(product_area = 'YouTube',total_tq_incremental_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_incremental_agreed, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_agreed, 0.0))) AS pitched_plus_num,

(SUM(IF(product_area = 'YouTube',total_tq_incremental_planned, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_incremental_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_incremental_agreed, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_planned, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_agreed, 0.0))) AS pitched_plus_den,

-- Agreed+
(SUM(IF(product_area = 'YouTube',total_tq_incremental_agreed, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_agreed, 0.0))) AS agreed_plus_num,

(
SUM(IF(product_area = 'YouTube',total_tq_incremental_planned, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_incremental_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_incremental_agreed, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_planned, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_pitched, 0.0))
+ SUM(IF(product_area = 'YouTube',total_tq_defense_agreed, 0.0))) AS agreed_plus_den,

(SUM(IF(product_area = 'YouTube',always_on_baseline, 0.0))
      + SUM(IF(product_area = 'YouTube',total_tq_incremental_agreed, 0.0))
      - SUM(IF(product_area = 'YouTube',total_tq_defense_planned, 0.0))
      - SUM(IF(product_area = 'YouTube',total_tq_defense_pitched, 0.0))) AS adjusted_baseline_and_agreed,

// Display Pipeline Metrics
SUM(IF(product_group = 'DVA Standard',pipeline_outlook,0.0)) AS display_pipeline_outlook,
SUM(IF(product_group = 'DVA Standard',safe_revenue,0.0)) AS display_secured_revenue,
SUM(IF(product_group = 'DVA Standard',always_on_baseline, 0.0)) AS display_always_on_baseline,
SUM(IF(product_group = 'DVA Standard',total_full_qtr_quota, 0.0)) AS display_quota,
SUM(IF(product_group = 'DVA Standard',total_tq_incremental_planned,0.0)) AS display_incremental_planned,
SUM(IF(product_group = 'DVA Standard',total_tq_defense_planned ,0.0)) AS display_defense_planned,

SUM(IF(product_group = 'DVA Standard',total_tq_incremental_planned, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_planned, 0.0)) AS display_total_planned,

(SUM(IF(product_group = 'DVA Standard',total_tq_incremental_pitched,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_pitched,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_incremental_agreed,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_agreed,0.0))) AS display_pitched_plus_num,

(SUM(IF(product_group = 'DVA Standard',total_tq_incremental_planned,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_incremental_pitched ,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_incremental_agreed,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_planned,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_pitched,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_agreed, 0.0))) AS display_pitched_plus_den,

(SUM(IF(product_group = 'DVA Standard',total_tq_incremental_agreed,0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_agreed,0.0))) AS display_agreed_plus_num,

(SUM(IF(product_group = 'DVA Standard',total_tq_incremental_planned, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_incremental_pitched, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_incremental_agreed, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_planned, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_pitched, 0.0))
+ SUM(IF(product_group = 'DVA Standard',total_tq_defense_agreed, 0.0))) AS display_agreed_plus_den,

(SUM(IF(product_group = 'DVA Standard',always_on_baseline,0.0))
  + SUM(IF(product_group = 'DVA Standard',total_tq_incremental_agreed,0.0))
  - SUM(IF(product_group = 'DVA Standard',total_tq_defense_planned,0.0))
  - SUM(IF(product_group = 'DVA Standard',total_tq_defense_pitched,0.0))) AS display_adjusted_baseline_and_agreed,


    FROM onegtm.alcs_pipeline_dash_2025.pipeline_calculations.root.latest AS _t
    WHERE
      (quarter = "2026-Q1")
      AND ADS_Revenue_Advertiser_Service_Channel ='LCS'
    GROUP BY ALL


),

YT_Secondary_Pipeline AS
(
SELECT
  parent_id,
  source_sub_pod_id AS sub_pod_id,
  SUM(pipeline_outlook) AS yt_secondary_pipeline_outlook,
  SUM(total_full_qtr_quota) AS yt_secondary_quota,
  SUM(safe_revenue) AS yt_secondary_secured_revenue,
  SUM(always_on_baseline) AS yt_secondary_always_on_baseline,


  -- Planned
SUM(total_tq_incremental_planned) AS yt_secondary_incremental_planned,
SUM(total_tq_defense_planned) AS yt_secondary_defense_planned,
SUM(total_tq_incremental_planned) + SUM(total_tq_defense_planned) AS yt_secondary_total_planned,

-- Pitched+
(SUM(total_tq_incremental_pitched) + SUM(total_tq_defense_pitched) + SUM(total_tq_incremental_agreed) + SUM(total_tq_defense_agreed)) AS yt_secondary_pitched_plus_num,

(SUM(total_tq_incremental_planned)
+ SUM(total_tq_incremental_pitched)
+ SUM(total_tq_incremental_agreed)
+ SUM(total_tq_defense_planned)
+ SUM(total_tq_defense_pitched)
+ SUM(total_tq_defense_agreed)
) AS yt_secondary_pitched_plus_den,

-- Agreed+
(SUM(total_tq_incremental_agreed) + SUM(total_tq_defense_agreed)) AS yt_secondary_agreed_plus_num,

(
SUM(total_tq_incremental_planned)
+ SUM(total_tq_incremental_pitched)
+ SUM(total_tq_incremental_agreed)
+ SUM(total_tq_defense_planned)
+ SUM(total_tq_defense_pitched)
+ SUM(total_tq_defense_agreed)
)
AS yt_secondary_agreed_plus_den,


      SUM(always_on_baseline)
      + SUM(total_tq_incremental_agreed)
      - SUM(total_tq_defense_planned)
      - SUM(total_tq_defense_pitched) AS yt_secondary_adjusted_baseline_and_agreed



  FROM
    onegtm.alcs_pipeline_dash_2025.pipeline_calculations.root.latest
    WHERE
      (quarter = "2026-Q1")
      AND ADS_Revenue_Advertiser_Service_Channel = 'LCS'
      AND is_yt_plus = TRUE
    GROUP BY ALL
),

ALL_Dimensions AS (
SELECT CAST(parent_id AS INT64) AS parent_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Pipeline_Base
UNION DISTINCT
SELECT CAST(parent_id AS INT64) AS parent_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM YT_Secondary_Pipeline
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

PT.* EXCEPT (sub_pod_id, parent_id),
YTSP.* EXCEPT (sub_pod_id, parent_id),
CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,

FROM All_Dimensions AS D

LEFT JOIN Customer_Dimensions AS CD
ON SAFE_CAST(D.parent_id AS STRING) = SAFE_CAST(CD.parent_id AS STRING)
AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(CD.sub_pod_id AS STRING)

LEFT OUTER JOIN Pipeline_Base AS PT
ON SAFE_CAST(D.parent_id AS STRING) = SAFE_CAST(PT.parent_id AS STRING)
AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(PT.sub_pod_id AS STRING)

LEFT OUTER JOIN YT_Secondary_Pipeline AS YTSP
ON SAFE_CAST(D.parent_id AS STRING) = SAFE_CAST(YTSP.parent_id AS STRING)
AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(YTSP.sub_pod_id AS STRING)

LEFT OUTER JOIN finance_segmentation AS FS
ON SAFE_CAST(CD.parent_id AS STRING) = SAFE_CAST(FS.parent_id AS STRING)
AND SAFE_CAST($SECTOR_REGION AS STRING) = SAFE_CAST(FS.sector_region AS STRING)

;

CREATE OR REPLACE TABLE $FULL_TABLE_NAME
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
