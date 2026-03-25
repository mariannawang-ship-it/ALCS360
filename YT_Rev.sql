DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.yt_revenue;


DEFINE MACRO YT_PLUS_TOTAL
product_area IN ('YouTube');
DEFINE MACRO YT_DVA
  product_area IN ('YouTube','DVA');
DEFINE MACRO YT_SECONDARY
product_group IN ('YouTube Brand','DVA D&V','YouTube D&A');

DEFINE MACRO YT_BRAND
Product_group IN ('YouTube Brand');
DEFINE MACRO DEMAND_GEN
(Product_group IN ('YouTube D&A') OR Product IN ('DVA DemandGen Campaign'));
DEFINE MACRO YT_DEMAND_GEN
Product_group IN ('YouTube D&A');
DEFINE MACRO DVA_DEMAND_GEN
Product IN ('DVA DemandGen Campaign');
DEFINE MACRO DVA_VIDEO
product IN ('Video');
DEFINE MACRO DVA_STANDARD
product_group IN ('DVA Standard');
DEFINE MACRO YT_PMAX
Product_group IN ('YouTube PMax+');
DEFINE MACRO DVA_PMAX
Product_group IN ('DVA PMax+');
DEFINE MACRO YT_APPS
Product_group IN ('YouTube App Promo');
DEFINE MACRO DVA_APPS
Product_group IN ('DVA App Promo');
DEFINE MACRO DV3
front_end IN ('DBM');  



DEFINE MACRO QUERY
WITH Revenue AS
(
  SELECT
    source_sub_pod_id AS sub_pod_id,
    division_id,

    //YT PLUS TOTAL
      SUM (IF($YT_PLUS_TOTAL, rev_qtd, 0.0)) AS yt_plus_total_rev_qtd, --QTD
      SUM (IF($YT_PLUS_TOTAL , rev_qtd_ly , 0.0)) AS yt_plus_total_rev_qtd_ly,  --QTD LY
      SUM (IF($YT_PLUS_TOTAL , rev_ytd , 0.0)) AS yt_plus_total_rev_ytd, --YTD
      SUM (IF($YT_PLUS_TOTAL , rev_ytd_ly , 0.0)) AS yt_plus_total_rev_ytd_ly, --YTD LY
      SUM (IF($YT_PLUS_TOTAL, eoqx_today , 0.0)) AS yt_plus_total_eoqx, --EOQX
      SUM (IF($YT_PLUS_TOTAL, quota_fullq , 0.0)) AS yt_plus_total_quota_fullq, --QUOTA
      SUM (IF($YT_PLUS_TOTAL, quota_qtd , 0.0)) AS yt_plus_total_quota_qtd, --QUOTA QTD
      SUM (IF($YT_PLUS_TOTAL, eoqx_today - quota_fullq , 0.0)) AS yt_plus_total_eoqx_surplus, --EOQX SURPLUS

//YT PLUS SECONDARY
      SUM (IF($YT_SECONDARY, rev_qtd, 0.0)) AS yt_secondary_rev_qtd, --QTD
      SUM (IF($YT_SECONDARY , rev_qtd_ly , 0.0)) AS yt_secondary_rev_qtd_ly,  --QTD LY
      SUM (IF($YT_SECONDARY , rev_ytd , 0.0)) AS yt_secondary_rev_ytd, --YTD
      SUM (IF($YT_SECONDARY , rev_ytd_ly , 0.0)) AS yt_secondary_rev_ytd_ly, --YTD LY
      SUM (IF($YT_SECONDARY, eoqx_today , 0.0)) AS yt_secondary_eoqx, --EOQX
      SUM (IF($YT_SECONDARY, quota_fullq , 0.0)) AS yt_secondary_quota_fullq, --QUOTA
      SUM (IF($YT_SECONDARY, quota_qtd , 0.0)) AS yt_secondary_quota_qtd, --QUOTA QTD
      SUM (IF($YT_SECONDARY, eoqx_today - quota_fullq , 0.0)) AS yt_secondary_eoqx_surplus, --EOQX SURPLUS      

    //YT BRAND
      SUM (IF($YT_BRAND, rev_qtd, 0.0)) AS yt_brand_rev_qtd,
      SUM (IF($YT_BRAND , rev_qtd_ly , 0.0)) AS yt_brand_rev_qtd_ly,
      SUM (IF($YT_BRAND , rev_ytd , 0.0)) AS yt_brand_rev_ytd,
      SUM (IF($YT_BRAND , rev_ytd_ly , 0.0)) AS yt_brand_rev_ytd_ly,
      SUM (IF($YT_BRAND, eoqx_today , 0.0)) AS yt_brand_eoqx, --EOQX
      SUM (IF($YT_BRAND, quota_fullq , 0.0)) AS yt_brand_quota_fullq, --QUOTA
      SUM (IF($YT_BRAND, quota_qtd , 0.0)) AS yt_brand_quota_qtd, --QUOTA QTD
      SUM (IF($YT_BRAND, eoqx_today - quota_fullq , 0.0)) AS yt_brand_eoqx_surplus, --EOQX SURPLUS      

    //DEMAND GEN
      SUM (IF($DEMAND_GEN, rev_qtd, 0.0)) AS dg_rev_qtd,
      SUM (IF($DEMAND_GEN , rev_qtd_ly , 0.0)) AS dg_rev_qtd_ly,
      SUM (IF($DEMAND_GEN , rev_ytd , 0.0)) AS dg_rev_ytd,
      SUM (IF($DEMAND_GEN , rev_ytd_ly , 0.0)) AS dg_rev_ytd_ly,
      SUM (IF($DEMAND_GEN, eoqx_today , 0.0)) AS dg_eoqx, --EOQX
      SUM (IF($DEMAND_GEN, quota_fullq , 0.0)) AS dg_quota_fullq, --QUOTA
      SUM (IF($DEMAND_GEN, quota_qtd , 0.0)) AS dg_quota_qtd, --QUOTA QTD
      SUM (IF($DEMAND_GEN, eoqx_today - quota_fullq , 0.0)) AS dg_eoqx_surplus, --EOQX SURPLUS      


    //YT DEMAND GEN
      SUM (IF($YT_DEMAND_GEN, rev_qtd, 0.0)) AS yt_dg_rev_qtd,
      SUM (IF($YT_DEMAND_GEN , rev_qtd_ly , 0.0)) AS yt_dg_rev_qtd_ly,
      SUM (IF($YT_DEMAND_GEN , rev_ytd , 0.0)) AS yt_dg_rev_ytd,
      SUM (IF($YT_DEMAND_GEN , rev_ytd_ly , 0.0)) AS yt_dg_rev_ytd_ly,  
      SUM (IF($YT_DEMAND_GEN, eoqx_today , 0.0)) AS yt_dg_eoqx, --EOQX
      SUM (IF($YT_DEMAND_GEN, quota_fullq , 0.0)) AS yt_dg_quota_fullq, --QUOTA
      SUM (IF($YT_DEMAND_GEN, quota_qtd , 0.0)) AS yt_dg_quota_qtd, --QUOTA QTD
      SUM (IF($YT_DEMAND_GEN, eoqx_today - quota_fullq , 0.0)) AS yt_dg_eoqx_surplus, --EOQX SURPLUS      

    //DVA DEMAND GEN
      SUM (IF($DVA_DEMAND_GEN, rev_qtd, 0.0)) AS dva_dg_rev_qtd,
      SUM (IF($DVA_DEMAND_GEN , rev_qtd_ly , 0.0)) AS dva_dg_rev_qtd_ly,
      SUM (IF($DVA_DEMAND_GEN , rev_ytd , 0.0)) AS dva_dg_rev_ytd,
      SUM (IF($DVA_DEMAND_GEN , rev_ytd_ly , 0.0)) AS dva_dg_rev_ytd_ly,
      SUM (IF($DVA_DEMAND_GEN, eoqx_today , 0.0)) AS dva_dg_eoqx, --EOQX
      SUM (IF($DVA_DEMAND_GEN, quota_fullq , 0.0)) AS dva_dg_quota_fullq, --QUOTA
      SUM (IF($DVA_DEMAND_GEN, quota_qtd , 0.0)) AS dva_dg_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_DEMAND_GEN, eoqx_today - quota_fullq , 0.0)) AS dva_dg_eoqx_surplus, --EOQX SURPLUS      

    //DVA VIDEO
      SUM (IF($DVA_VIDEO, rev_qtd, 0.0)) AS dva_video_rev_qtd,
      SUM (IF($DVA_VIDEO , rev_qtd_ly , 0.0)) AS dva_video_rev_qtd_ly,
      SUM (IF($DVA_VIDEO , rev_ytd , 0.0)) AS dva_video_rev_ytd,
      SUM (IF($DVA_VIDEO , rev_ytd_ly , 0.0)) AS dva_video_rev_ytd_ly,
      SUM (IF($DVA_VIDEO, eoqx_today , 0.0)) AS dva_video_eoqx, --EOQX
      SUM (IF($DVA_VIDEO, quota_fullq , 0.0)) AS dva_video_quota_fullq, --QUOTA
      SUM (IF($DVA_VIDEO, quota_qtd , 0.0)) AS dva_video_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_VIDEO, eoqx_today - quota_fullq , 0.0)) AS dva_video_eoqx_surplus, --EOQX SURPLUS      

    //DVA STANDARD
      SUM (IF($DVA_STANDARD, rev_qtd, 0.0)) AS dva_standard_rev_qtd,
      SUM (IF($DVA_STANDARD , rev_qtd_ly , 0.0)) AS dva_standard_rev_qtd_ly,
      SUM (IF($DVA_STANDARD , rev_ytd , 0.0)) AS dva_standard_rev_ytd,
      SUM (IF($DVA_STANDARD , rev_ytd_ly , 0.0)) AS dva_standard_rev_ytd_ly,
      SUM (IF($DVA_STANDARD, eoqx_today , 0.0)) AS dva_standard_eoqx, --EOQX
      SUM (IF($DVA_STANDARD, quota_fullq , 0.0)) AS dva_standard_quota_fullq, --QUOTA
      SUM (IF($DVA_STANDARD, quota_qtd , 0.0)) AS dva_standard_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_STANDARD, eoqx_today - quota_fullq , 0.0)) AS dva_standard_eoqx_surplus, --EOQX SURPLUS      

    //YT PMAX
      SUM (IF($YT_PMAX, rev_qtd, 0.0)) AS yt_pmax_rev_qtd,
      SUM (IF($YT_PMAX , rev_qtd_ly , 0.0)) AS yt_pmax_rev_qtd_ly,
      SUM (IF($YT_PMAX , rev_ytd , 0.0)) AS yt_pmax_rev_ytd,
      SUM (IF($YT_PMAX , rev_ytd_ly , 0.0)) AS yt_pmax_rev_ytd_ly,
      SUM (IF($YT_PMAX, eoqx_today , 0.0)) AS yt_pmax_eoqx, --EOQX
      SUM (IF($YT_PMAX, quota_fullq , 0.0)) AS yt_pmax_quota_fullq, --QUOTA
      SUM (IF($YT_PMAX, quota_qtd , 0.0)) AS yt_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($YT_PMAX, eoqx_today - quota_fullq , 0.0)) AS yt_pmax_eoqx_surplus, --EOQX SURPLUS      

    //DVA PMAX
      SUM (IF($DVA_PMAX, rev_qtd, 0.0)) AS dva_pmax_rev_qtd,
      SUM (IF($DVA_PMAX , rev_qtd_ly , 0.0)) AS dva_pmax_rev_qtd_ly,
      SUM (IF($DVA_PMAX , rev_ytd , 0.0)) AS dva_pmax_rev_ytd,
      SUM (IF($DVA_PMAX , rev_ytd_ly , 0.0)) AS dva_pmax_rev_ytd_ly,
      SUM (IF($DVA_PMAX, eoqx_today , 0.0)) AS dva_pmax_eoqx, --EOQX
      SUM (IF($DVA_PMAX, quota_fullq , 0.0)) AS dva_pmax_quota_fullq, --QUOTA
      SUM (IF($DVA_PMAX, quota_qtd , 0.0)) AS dva_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_PMAX, eoqx_today - quota_fullq , 0.0)) AS dva_pmax_eoqx_surplus, --EOQX SURPLUS      

    //YT APPS
      SUM (IF($YT_APPS, rev_qtd, 0.0)) AS yt_apps_rev_qtd,
      SUM (IF($YT_APPS , rev_qtd_ly , 0.0)) AS yt_apps_rev_qtd_ly,
      SUM (IF($YT_APPS , rev_ytd , 0.0)) AS yt_apps_rev_ytd,
      SUM (IF($YT_APPS , rev_ytd_ly , 0.0)) AS yt_apps_rev_ytd_ly,
      SUM (IF($YT_APPS, eoqx_today , 0.0)) AS yt_apps_eoqx, --EOQX
      SUM (IF($YT_APPS, quota_fullq , 0.0)) AS yt_apps_quota_fullq, --QUOTA
      SUM (IF($YT_APPS, quota_qtd , 0.0)) AS yt_apps_quota_qtd, --QUOTA QTD
      SUM (IF($YT_APPS, eoqx_today - quota_fullq , 0.0)) AS yt_apps_eoqx_surplus, --EOQX SURPLUS      

    //DVA APPS
      SUM (IF($DVA_APPS, rev_qtd, 0.0)) AS dva_apps_rev_qtd,
      SUM (IF($DVA_APPS , rev_qtd_ly , 0.0)) AS dva_apps_rev_qtd_ly,
      SUM (IF($DVA_APPS , rev_ytd , 0.0)) AS dva_apps_rev_ytd,
      SUM (IF($DVA_APPS , rev_ytd_ly , 0.0)) AS dva_apps_rev_ytd_ly,
      SUM (IF($DVA_APPS, eoqx_today , 0.0)) AS dva_apps_eoqx, --EOQX
      SUM (IF($DVA_APPS, quota_fullq , 0.0)) AS dva_apps_quota_fullq, --QUOTA
      SUM (IF($DVA_APPS, quota_qtd , 0.0)) AS dva_apps_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_APPS, eoqx_today - quota_fullq , 0.0)) AS dva_apps_eoqx_surplus, --EOQX SURPLUS      

    //DV3 TOTAL
      SUM (IF($YT_PLUS_TOTAL AND $DV3, rev_qtd, 0.0)) AS dv3_yt_plus_total_rev_qtd, --QTD
      SUM (IF($YT_PLUS_TOTAL AND $DV3, rev_qtd_ly , 0.0)) AS dv3_yt_plus_total_rev_qtd_ly,  --QTD LY
      SUM (IF($YT_PLUS_TOTAL AND $DV3, rev_ytd , 0.0)) AS dv3_yt_plus_total_rev_ytd, --YTD
      SUM (IF($YT_PLUS_TOTAL AND $DV3, rev_ytd_ly , 0.0)) AS dv3_yt_plus_total_rev_ytd_ly, --YTD LY     
      SUM (IF($YT_PLUS_TOTAL AND $DV3, eoqx_today , 0.0)) AS dv3_yt_plus_total_eoqx, --EOQX
      SUM (IF($YT_PLUS_TOTAL AND $DV3, quota_fullq , 0.0)) AS dv3_yt_plus_total_quota_fullq, --QUOTA
      SUM (IF($YT_PLUS_TOTAL AND $DV3, quota_qtd , 0.0)) AS dv3_yt_plus_total_quota_qtd, --QUOTA QTD
      SUM (IF($YT_PLUS_TOTAL AND $DV3, eoqx_today - quota_fullq , 0.0)) AS dv3_yt_plus_total_eoqx_surplus, --EOQX SURPLUS       

      FROM 
        americas_salesfinance_dremel.amer_weekly
        WHERE service_channel = 'LCS'
          AND region = 'Americas'
      GROUP BY ALL

)

SELECT * FROM Revenue
;

CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
