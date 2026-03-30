DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.apps_revenue;


DEFINE MACRO APPS
product_group IN ('Search App Promo','YouTube App Promo','DVA App Promo');
DEFINE MACRO APPS_SEARCH
product_area = 'Search' AND Product_group IN ('Search App Promo');
DEFINE MACRO APPS_YT
product_area = 'YouTube' AND Product_group IN ('YouTube App Promo');
DEFINE MACRO APPS_DVA
product_area = 'DVA' AND Product_group IN ('DVA App Promo');


DEFINE MACRO QUERY
WITH Revenue AS
(
  SELECT
    source_sub_pod_id AS sub_pod_id,
    division_id,

    //APPS_TOTAL
      SUM (IF($APPS, rev_qtd, 0.0)) AS apps_total_rev_qtd, --QTD
      SUM (IF($APPS , rev_qtd_ly , 0.0)) AS apps_total_rev_qtd_ly,  --QTD LY
      SUM (IF($APPS , rev_ytd , 0.0)) AS apps_total_rev_ytd, --YTD
      SUM (IF($APPS , rev_ytd_ly , 0.0)) AS apps_total_rev_ytd_ly, --YTD LY
      SUM (IF($APPS, eoqx_today , 0.0)) AS apps_total_eoqx, --EOQX
      SUM (IF($APPS, quota_fullq , 0.0)) AS apps_total_quota_fullq, --QUOTA
      SUM (IF($APPS, quota_qtd , 0.0)) AS apps_total_quota_qtd, --QUOTA QTD
      SUM (IF($APPS, eoqx_today - quota_fullq , 0.0)) AS apps_total_eoqx_surplus, --EOQX SURPLUS

    //APPS SEARCH
      SUM (IF($APPS_SEARCH, rev_qtd, 0.0)) AS apps_search_text_rev_qtd,
      SUM (IF($APPS_SEARCH , rev_qtd_ly , 0.0)) AS apps_search_text_rev_qtd_ly,
      SUM (IF($APPS_SEARCH , rev_ytd , 0.0)) AS apps_search_text_rev_ytd,
      SUM (IF($APPS_SEARCH , rev_ytd_ly , 0.0)) AS apps_search_text_rev_ytd_ly,
      SUM (IF($APPS_SEARCH, eoqx_today , 0.0)) AS apps_search_text_eoqx, --EOQX
      SUM (IF($APPS_SEARCH, quota_fullq , 0.0)) AS apps_search_text_quota_fullq, --QUOTA
      SUM (IF($APPS_SEARCH, quota_qtd , 0.0)) AS apps_search_text_quota_qtd, --QUOTA QTD
      SUM (IF($APPS_SEARCH, eoqx_today - quota_fullq , 0.0)) AS apps_search_text_eoqx_surplus, --EOQX SURPLUS      

    //APPS YT
      SUM (IF($APPS_YT, rev_qtd, 0.0)) AS apps_yt_pmax_rev_qtd,
      SUM (IF($APPS_YT , rev_qtd_ly , 0.0)) AS apps_yt_pmax_rev_qtd_ly,
      SUM (IF($APPS_YT , rev_ytd , 0.0)) AS apps_yt_pmax_rev_ytd,
      SUM (IF($APPS_YT , rev_ytd_ly , 0.0)) AS apps_yt_pmax_rev_ytd_ly,  
      SUM (IF($APPS_YT, eoqx_today , 0.0)) AS apps_yt_pmax_eoqx, --EOQX
      SUM (IF($APPS_YT, quota_fullq , 0.0)) AS apps_yt_pmax_quota_fullq, --QUOTA
      SUM (IF($APPS_YT, quota_qtd , 0.0)) AS apps_yt_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($APPS_YT, eoqx_today - quota_fullq , 0.0)) AS apps_yt_pmax_eoqx_surplus, --EOQX SURPLUS      

    //APPS DVA
      SUM (IF($APPS_DVA, rev_qtd, 0.0)) AS apps_dva_rev_qtd,
      SUM (IF($APPS_DVA , rev_qtd_ly , 0.0)) AS apps_dva_rev_qtd_ly,
      SUM (IF($APPS_DVA , rev_ytd , 0.0)) AS apps_dva_rev_ytd,
      SUM (IF($APPS_DVA , rev_ytd_ly , 0.0)) AS apps_dva_rev_ytd_ly,
      SUM (IF($APPS_DVA, eoqx_today , 0.0)) AS apps_dva_eoqx, --EOQX
      SUM (IF($APPS_DVA, quota_fullq , 0.0)) AS apps_dva_quota_fullq, --QUOTA
      SUM (IF($APPS_DVA, quota_qtd , 0.0)) AS apps_dva_quota_qtd, --QUOTA QTD
      SUM (IF($APPS_DVA, eoqx_today - quota_fullq , 0.0)) AS apps_dva_eoqx_surplus, --EOQX SURPLUS      

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

GRANT OWNER ON TABLE $FULL_TABLE_NAME TO 'anveshkaaram@google.com','anveshkaaram@prod.google.com','sanchitagarg@google.com', 'sanchitagarg@prod.google.com', 'mariannawang@google.com', 'mariannawang@prod.google.com';
