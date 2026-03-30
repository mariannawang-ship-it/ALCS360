DEFINE MACRO FULL_TABLE_NAME avgtm.play_reporting_2026.q1.apps_combined_table;


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

/*
Pipeline_Table AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.apps_pipeline
),
*/

Revenue AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.apps_revenue
),

BFM AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.apps_bfm
),

BFM_Targets AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.apps_bfm_targets
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

activation_map AS
(
  SELECT 
    DISTINCT
    division_id,
    country_code,
    apps
   FROM avgtm.play_reporting_2026.q1.activation_map
),

ALL_Dimensions AS (
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Revenue
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM_Targets
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



  //PT.* EXCEPT (sub_pod_id, division_id),
  R.* EXCEPT (sub_pod_id, division_id),
  B.* EXCEPT (sub_pod_id, division_id),
  B_T.* EXCEPT (sub_pod_id, division_id),
  CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,
  CASE WHEN AM.apps = TRUE THEN TRUE ELSE FALSE END AS apps

FROM  ALL_Dimensions AS D
  
  LEFT JOIN Customer_Dimensions AS CD
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(CD.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(CD.sub_pod_id AS STRING)

/*  LEFT OUTER JOIN Pipeline_Table AS PT
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(PT.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(PT.sub_pod_id AS STRING)
*/  

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

)

SELECT * FROM Combined_Base;

DEFINE MACRO CNS_PATH
  /cns/iq-d/home/avgtm/avgtm.play_reporting_2026/q1/apps_combined_table;

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
