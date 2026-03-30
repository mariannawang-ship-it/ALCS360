DEFINE MACRO FULL_TABLE_NAME avgtm.play_reporting_2026.q1.measurement_combined_table;


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

BFM AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.measurement_bfm
),

BFM_Targets AS
(
SELECT * FROM avgtm.play_reporting_2026.q1.measurement_bfm_targets
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
    CASE WHEN (measurement_online_only = TRUE OR measurement_online_offline = TRUE
      OR measurement_offline_only = TRUE) THEN TRUE ELSE FALSE END AS measurement_data_strength,
    measurement_online_only,
    measurement_offline_only,
    measurement_online_offline,
    measurement_mmm,
    measurement_incrementality,
    measurement_online_gtg
   FROM avgtm.play_reporting_2026.q1.activation_map
),

Pipeline_Table AS
(
  SELECT
    division_id,
    sub_pod_id,

// Offering Value $    
    SUM(impact_available) AS total_available_offering_value,
    SUM(impact_dismissed) AS total_dismissed_offering_value,
    SUM(impact_planned) AS total_planned_offering_value,
    SUM(impact_pitched) AS total_pitched_offering_value,
    SUM(impact_agreed) AS total_agreed_offering_value,
    SUM(impact_implemented) AS total_implemented_offering_value,
    SUM(impact_lost) AS total_lost_offering_value,
    SUM(impact_abandoned) AS total_abandoned_offering_value,

    SUM(IF(offering_type_group = 'Data Strength', impact_available, 0)) AS available_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_dismissed, 0)) AS dismissed_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_planned, 0)) AS planned_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_pitched, 0)) AS pitched_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_agreed, 0)) AS agreed_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_implemented, 0)) AS implemented_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_lost, 0)) AS lost_data_strength_offering_value,
    SUM(IF(offering_type_group = 'Data Strength', impact_abandoned, 0)) AS abandoned_data_strength_offering_value,

    SUM(IF(offering_type_group = 'MMM', impact_available, 0)) AS available_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_dismissed, 0)) AS dismissed_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_planned, 0)) AS planned_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_pitched, 0)) AS pitched_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_agreed, 0)) AS agreed_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_implemented, 0)) AS implemented_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_lost, 0)) AS lost_mmm_offering_value,
    SUM(IF(offering_type_group = 'MMM', impact_abandoned, 0)) AS abandoned_mmm_offering_value,

    SUM(IF(offering_type_group = 'Incrementality', impact_available, 0)) AS available_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_dismissed, 0)) AS dismissed_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_planned, 0)) AS planned_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_pitched, 0)) AS pitched_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_agreed, 0)) AS agreed_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_implemented, 0)) AS implemented_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_lost, 0)) AS lost_incrementality_offering_value,
    SUM(IF(offering_type_group = 'Incrementality', impact_abandoned, 0)) AS abandoned_incrementality_offering_value,


    COUNT(product_action) AS available_offering_count,
    COUNT(IF(offering_stage = 'DISMISSED', product_action, NULL)) AS dismissed_offering_count,
    COUNT(IF(offering_stage = 'PLANNED', product_action, NULL)) AS planned_offering_count,
    COUNT(IF(offering_stage IN ('PITCHED'), product_action, NULL)) AS pitched_offering_count,
    COUNT(IF(offering_stage = 'AGREED', product_action, NULL)) AS agreed_offering_count,
    COUNT(IF(offering_stage = 'WON', product_action, NULL)) AS implemented_offering_count,
    COUNT(IF(offering_stage = 'LOST', product_action, NULL)) AS lost_offering_count,
    COUNT(IF(offering_stage = 'ABANDONED', product_action, NULL)) AS abandoned_offering_count,
    COUNT(IF(offering_stage = 'PROPOSED', campaign_id, NULL)) AS recommended_offering_count,



// Offering Count
    COUNT(IF(offering_type_group = 'Data Strength', product_action, NULL)) AS available_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'DISMISSED', product_action, NULL)) AS dismissed_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'PLANNED', product_action, NULL)) AS planned_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage IN ('PITCHED'), product_action, NULL)) AS pitched_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'AGREED', product_action, NULL)) AS agreed_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'WON', product_action, NULL)) AS implemented_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'LOST', product_action, NULL)) AS lost_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'ABANDONED', product_action, NULL)) AS abandoned_data_strength_offering_count,
    COUNT(IF(offering_type_group = 'Data Strength' AND offering_stage = 'PROPOSED', campaign_id, NULL)) AS recommended_data_strength_offering_count,

    COUNT(IF(offering_type_group = 'MMM', product_action, NULL)) AS available_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'DISMISSED', product_action, NULL)) AS dismissed_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'PLANNED', product_action, NULL)) AS planned_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage IN ('PITCHED'), product_action, NULL)) AS pitched_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'AGREED', product_action, NULL)) AS agreed_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'WON', product_action, NULL)) AS implemented_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'LOST', product_action, NULL)) AS lost_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'ABANDONED', product_action, NULL)) AS abandoned_mmm_offering_count,
    COUNT(IF(offering_type_group = 'MMM' AND offering_stage = 'PROPOSED', campaign_id, NULL)) AS recommended_mmm_offering_count,

    COUNT(IF(offering_type_group = 'Incrementality', product_action, NULL)) AS available_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'DISMISSED', product_action, NULL)) AS dismissed_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'PLANNED', product_action, NULL)) AS planned_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage IN ('PITCHED'), campaign_id, NULL)) AS pitched_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'AGREED', product_action, NULL)) AS agreed_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'WON', product_action, NULL)) AS implemented_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'LOST', product_action, NULL)) AS lost_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'ABANDONED', product_action, NULL)) AS abandoned_incrementality_offering_count,
    COUNT(IF(offering_type_group = 'Incrementality' AND offering_stage = 'PROPOSED', campaign_id, NULL)) AS recommended_incrementality_offering_count,

    COUNT(IF(offering_type_group = 'Value Delivered', product_action, NULL)) AS available_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'DISMISSED', product_action, NULL)) AS dismissed_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'PLANNED', product_action, NULL)) AS planned_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage IN ('PITCHED'), product_action, NULL)) AS pitched_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'AGREED', product_action, NULL)) AS agreed_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'WON', product_action, NULL)) AS implemented_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'LOST', product_action, NULL)) AS lost_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'ABANDONED', product_action, NULL)) AS abandoned_value_delivered_offering_count,
    COUNT(IF(offering_type_group = 'Value Delivered' AND offering_stage = 'PROPOSED', campaign_id, NULL)) AS recommended_value_delivered_offering_count,


// Opportunity Count 
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength', opportunity_id, NULL)) AS available_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'DISMISSED', opportunity_id, NULL)) AS dismissed_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'PLANNED', opportunity_id, NULL)) AS planned_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage IN ('PITCHED'), opportunity_id, NULL)) AS pitched_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'AGREED', opportunity_id, NULL)) AS agreed_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'WON', opportunity_id, NULL)) AS implemented_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'LOST', opportunity_id, NULL)) AS lost_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'ABANDONED', opportunity_id, NULL)) AS abandoned_data_strength_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Data Strength' AND offering_stage = 'PROPOSED', opportunity_id, NULL)) AS recommended_data_strength_opportunity_count,

    COUNT(DISTINCT IF(offering_type_group = 'MMM', opportunity_id, NULL)) AS available_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'DISMISSED', opportunity_id, NULL)) AS dismissed_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'PLANNED', opportunity_id, NULL)) AS planned_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage IN ('PITCHED'), opportunity_id, NULL)) AS pitched_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'AGREED', opportunity_id, NULL)) AS agreed_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'WON', opportunity_id, NULL)) AS implemented_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'LOST', opportunity_id, NULL)) AS lost_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'ABANDONED', opportunity_id, NULL)) AS abandoned_mmm_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'MMM' AND offering_stage = 'PROPOSED', opportunity_id, NULL)) AS recommended_mmm_opportunity_count,

    COUNT(DISTINCT IF(offering_type_group = 'Incrementality', opportunity_id, NULL)) AS available_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'DISMISSED', opportunity_id, NULL)) AS dismissed_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'PLANNED', opportunity_id, NULL)) AS planned_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage IN ('PITCHED'), opportunity_id, NULL)) AS pitched_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'AGREED', opportunity_id, NULL)) AS agreed_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'WON', opportunity_id, NULL)) AS implemented_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'LOST', opportunity_id, NULL)) AS lost_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'ABANDONED', opportunity_id, NULL)) AS abandoned_incrementality_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Incrementality' AND offering_stage = 'PROPOSED', opportunity_id, NULL)) AS recommended_incrementality_opportunity_count,

    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered', opportunity_id, NULL)) AS available_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'DISMISSED', opportunity_id, NULL)) AS dismissed_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'PLANNED', opportunity_id, NULL)) AS planned_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage IN ('PITCHED'), opportunity_id, NULL)) AS pitched_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'AGREED', opportunity_id, NULL)) AS agreed_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered', opportunity_id, NULL)) AS implemented_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'LOST', opportunity_id, NULL)) AS lost_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'ABANDONED', opportunity_id, NULL)) AS abandoned_value_delivered_opportunity_count,
    COUNT(DISTINCT IF(offering_type_group = 'Value Delivered' AND offering_stage = 'PROPOSED', opportunity_id, NULL)) AS recommended_value_delivered_opportunity_count


FROM avgtm.play_reporting_2026.q1.measurement_pipeline
GROUP BY ALL    
),


ALL_Dimensions AS (
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM BFM_Targets
UNION DISTINCT
SELECT CAST(division_id AS INT64) AS division_id, SAFE_CAST(sub_pod_id AS STRING) AS sub_pod_id FROM Pipeline_Table
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

  PT.* EXCEPT (sub_pod_id, division_id),
  B.* EXCEPT (sub_pod_id, division_id),
  B_T.* EXCEPT (sub_pod_id, division_id),
  CASE WHEN FS.whtt IS NULL THEN '4-Other' ELSE FS.whtt END AS whtt ,

  CASE WHEN AM.measurement_data_strength = TRUE THEN TRUE ELSE FALSE END AS measurement_data_strength,
  CASE WHEN AM.measurement_online_only = TRUE THEN TRUE ELSE FALSE END AS measurement_online_only,
  CASE WHEN AM.measurement_offline_only = TRUE THEN TRUE ELSE FALSE END AS measurement_offline_only,
  CASE WHEN AM.measurement_online_offline = TRUE THEN TRUE ELSE FALSE END AS measurement_online_offline,
  CASE WHEN AM.measurement_mmm = TRUE THEN TRUE ELSE FALSE END AS measurement_mmm,
  CASE WHEN AM.measurement_incrementality = TRUE THEN TRUE ELSE FALSE END AS measurement_incrementality,
  CASE WHEN AM.measurement_online_gtg = TRUE THEN TRUE ELSE FALSE END AS measurement_online_gtg


FROM  ALL_Dimensions AS D
  
  LEFT JOIN Customer_Dimensions AS CD
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(CD.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(CD.sub_pod_id AS STRING)

  LEFT OUTER JOIN Pipeline_Table AS PT
  ON SAFE_CAST(D.division_id AS STRING) = SAFE_CAST(PT.division_id AS STRING)
  AND SAFE_CAST(D.sub_pod_id AS STRING) = SAFE_CAST(PT.sub_pod_id AS STRING)
  
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
  /cns/iq-d/home/avgtm/avgtm.play_reporting_2026/q1/measurement_combined_table;

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
