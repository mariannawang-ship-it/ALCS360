SET QueryRequest.accounting_group = "avgtm";
RUN amer_gtm_macros_1();
DEFINE MACRO FULL_TABLE_NAME
    avgtm.play_reporting_2026.q1.search_revenue;

DEFINE MACRO REPORTING_DATE
  DATE_ADD(CAST(CURRENT_TIMESTAMP() AS date), INTERVAL -1 DAY);

DEFINE MACRO QTD
  date BETWEEN DATE_TRUNC($REPORTING_DATE, QUARTER) AND $REPORTING_DATE;

DEFINE MACRO QTD_LY
  date BETWEEN DATE_SUB(DATE_TRUNC($REPORTING_DATE, QUARTER), INTERVAL 1 YEAR) AND DATE_SUB($REPORTING_DATE, INTERVAL 1 YEAR);
  
DEFINE MACRO YTD
  date BETWEEN DATE_TRUNC($REPORTING_DATE, YEAR) AND $REPORTING_DATE;

DEFINE MACRO YTD_LY
  date BETWEEN DATE_SUB(DATE_TRUNC($REPORTING_DATE, YEAR), INTERVAL 1 YEAR) AND DATE_SUB($REPORTING_DATE, INTERVAL 1 YEAR);
  

DEFINE MACRO SEARCH
product_area = 'Search';
DEFINE MACRO SEARCH_TEXT
product_area = 'Search' AND Product_group IN ('Search Standard') AND product IN ('Standard Text');
DEFINE MACRO SEARCH_PMAX
product_area = 'Search' AND Product_group IN ('Search PMax+');
DEFINE MACRO SEARCH_APP
product_area = 'Search' AND Product_group IN ('Search App Promo');
DEFINE MACRO SEARCH_SHOPPING
product_area = 'Search' AND Product_group IN ('Search Standard') AND product IN ('Shopping');
DEFINE MACRO SEARCH_OTHERS
product_area = 'Search' AND Product_group IN ('Search Standard') AND product NOT IN ('Standard Text','Shopping');
DEFINE MACRO YT_PMAX
product_area = 'YouTube' AND Product_group IN ('YouTube PMax+');
DEFINE MACRO DVA_PMAX
product_area = 'DVA' AND Product_group IN ('DVA PMax+');

DEFINE MACRO SEARCH_PLUS
  product_group IN ('Search Standard','Search PMax+','DVA PMax+', 'YouTube PMax+');

DEFINE MACRO QUERY
WITH Revenue AS
(
  SELECT
    source_sub_pod_id AS sub_pod_id,
    division_id,

    //SEARCH PLUS (Includes YT PMAX and DVA PMAX)
      SUM (IF($SEARCH_PLUS, rev_qtd, 0.0)) AS search_total_rev_qtd, --QTD
      SUM (IF($SEARCH_PLUS , rev_qtd_ly , 0.0)) AS search_total_rev_qtd_ly,  --QTD LY
      SUM (IF($SEARCH_PLUS , rev_ytd , 0.0)) AS search_total_rev_ytd, --YTD
      SUM (IF($SEARCH_PLUS , rev_ytd_ly , 0.0)) AS search_total_rev_ytd_ly, --YTD LY
      SUM (IF($SEARCH_PLUS, eoqx_today , 0.0)) AS search_total_eoqx, --EOQX
      SUM (IF($SEARCH_PLUS, quota_fullq , 0.0)) AS search_total_quota_fullq, --QUOTA
      SUM (IF($SEARCH_PLUS, quota_qtd , 0.0)) AS search_total_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_PLUS, eoqx_today - quota_fullq , 0.0)) AS search_total_eoqx_surplus, --EOQX SURPLUS


    //SEARCH TEXT
      SUM (IF($SEARCH_TEXT, rev_qtd, 0.0)) AS search_text_rev_qtd,
      SUM (IF($SEARCH_TEXT , rev_qtd_ly , 0.0)) AS search_text_rev_qtd_ly,
      SUM (IF($SEARCH_TEXT , rev_ytd , 0.0)) AS search_text_rev_ytd,
      SUM (IF($SEARCH_TEXT , rev_ytd_ly , 0.0)) AS search_text_rev_ytd_ly,
      SUM (IF($SEARCH_TEXT, eoqx_today , 0.0)) AS search_text_eoqx, --EOQX
      SUM (IF($SEARCH_TEXT, quota_fullq , 0.0)) AS search_text_quota_fullq, --QUOTA
      SUM (IF($SEARCH_TEXT, quota_qtd , 0.0)) AS search_text_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_TEXT, eoqx_today - quota_fullq , 0.0)) AS search_text_eoqx_surplus, --EOQX SURPLUS

    //SEARCH PMAX
      SUM (IF($SEARCH_PMAX, rev_qtd, 0.0)) AS search_pmax_rev_qtd,
      SUM (IF($SEARCH_PMAX , rev_qtd_ly , 0.0)) AS search_pmax_rev_qtd_ly,
      SUM (IF($SEARCH_PMAX , rev_ytd , 0.0)) AS search_pmax_rev_ytd,
      SUM (IF($SEARCH_PMAX , rev_ytd_ly , 0.0)) AS search_pmax_rev_ytd_ly,
      SUM (IF($SEARCH_PMAX, eoqx_today , 0.0)) AS search_pmax_eoqx, --EOQX
      SUM (IF($SEARCH_PMAX, quota_fullq , 0.0)) AS search_pmax_quota_fullq, --QUOTA        
      SUM (IF($SEARCH_PMAX, quota_qtd , 0.0)) AS search_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_PMAX, eoqx_today - quota_fullq , 0.0)) AS search_pmax_eoqx_surplus, --EOQX SURPLUS

    //SEARCH APP
      SUM (IF($SEARCH_APP, rev_qtd, 0.0)) AS search_app_rev_qtd,
      SUM (IF($SEARCH_APP , rev_qtd_ly , 0.0)) AS search_app_rev_qtd_ly,
      SUM (IF($SEARCH_APP , rev_ytd , 0.0)) AS search_app_rev_ytd,
      SUM (IF($SEARCH_APP , rev_ytd_ly , 0.0)) AS search_app_rev_ytd_ly,
      SUM (IF($SEARCH_APP, eoqx_today , 0.0)) AS search_app_eoqx, --EOQX
      SUM (IF($SEARCH_APP, quota_fullq , 0.0)) AS search_app_quota_fullq, --QUOTA      
      SUM (IF($SEARCH_APP, quota_qtd , 0.0)) AS search_app_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_APP, eoqx_today - quota_fullq , 0.0)) AS search_app_eoqx_surplus, --EOQX SURPLUS      

    //SEARCH SHOPPING
      SUM (IF($SEARCH_SHOPPING, rev_qtd, 0.0)) AS search_shopping_rev_qtd,
      SUM (IF($SEARCH_SHOPPING , rev_qtd_ly , 0.0)) AS search_shopping_rev_qtd_ly,
      SUM (IF($SEARCH_SHOPPING , rev_ytd , 0.0)) AS search_shopping_rev_ytd,
      SUM (IF($SEARCH_SHOPPING , rev_ytd_ly , 0.0)) AS search_shopping_rev_ytd_ly,
      SUM (IF($SEARCH_SHOPPING, eoqx_today , 0.0)) AS search_shopping_eoqx, --EOQX
      SUM (IF($SEARCH_SHOPPING, quota_fullq , 0.0)) AS search_shopping_quota_fullq, --QUOTA      
      SUM (IF($SEARCH_SHOPPING, quota_qtd , 0.0)) AS search_shopping_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_SHOPPING, eoqx_today - quota_fullq , 0.0)) AS search_shopping_eoqx_surplus, --EOQX SURPLUS      

    //SEARCH OTHERS
      SUM (IF($SEARCH_OTHERS, rev_qtd, 0.0)) AS search_others_rev_qtd,
      SUM (IF($SEARCH_OTHERS , rev_qtd_ly , 0.0)) AS search_others_rev_qtd_ly,
      SUM (IF($SEARCH_OTHERS , rev_ytd , 0.0)) AS search_others_rev_ytd,
      SUM (IF($SEARCH_OTHERS , rev_ytd_ly , 0.0)) AS search_others_rev_ytd_ly,
      SUM (IF($SEARCH_OTHERS, eoqx_today , 0.0)) AS search_others_eoqx, --EOQX
      SUM (IF($SEARCH_OTHERS, quota_fullq , 0.0)) AS search_others_quota_fullq, --QUOTA      
      SUM (IF($SEARCH_OTHERS, quota_qtd , 0.0)) AS search_others_quota_qtd, --QUOTA QTD
      SUM (IF($SEARCH_OTHERS, eoqx_today - quota_fullq , 0.0)) AS search_others_eoqx_surplus, --EOQX SURPLUS   

      //YT PMAX
      SUM (IF($YT_PMAX, rev_qtd, 0.0)) AS yt_pmax_rev_qtd,
      SUM (IF($YT_PMAX , rev_qtd_ly , 0.0)) AS yt_pmax_rev_qtd_ly,
      SUM (IF($YT_PMAX , rev_ytd , 0.0)) AS yt_pmax_rev_ytd,
      SUM (IF($YT_PMAX , rev_ytd_ly , 0.0)) AS yt_pmax_rev_ytd_ly,
      SUM (IF($YT_PMAX, eoqx_today , 0.0)) AS yt_pmax_eoqx, --EOQX
      SUM (IF($YT_PMAX, quota_fullq , 0.0)) AS yt_pmax_quota_fullq, --QUOTA      
      SUM (IF($YT_PMAX, quota_qtd , 0.0)) AS yt_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($YT_PMAX, eoqx_today - quota_fullq , 0.0)) AS yt_pmax_eoqx_surplus, --EOQX SURPLUS

      //DVA APP
      SUM (IF($DVA_PMAX, rev_qtd, 0.0)) AS dva_pmax_rev_qtd,
      SUM (IF($DVA_PMAX , rev_qtd_ly , 0.0)) AS dva_pmax_rev_qtd_ly,
      SUM (IF($DVA_PMAX , rev_ytd , 0.0)) AS dva_pmax_rev_ytd,
      SUM (IF($DVA_PMAX , rev_ytd_ly , 0.0)) AS dva_pmax_rev_ytd_ly,
      SUM (IF($DVA_PMAX, eoqx_today , 0.0)) AS dva_pmax_eoqx, --EOQX
      SUM (IF($DVA_PMAX, quota_fullq , 0.0)) AS dva_pmax_quota_fullq, --QUOTA
      SUM (IF($DVA_PMAX, quota_qtd , 0.0)) AS dva_pmax_quota_qtd, --QUOTA QTD
      SUM (IF($DVA_PMAX, eoqx_today - quota_fullq , 0.0)) AS dva_pmax_eoqx_surplus --EOQX SURPLUS


      FROM 
        americas_salesfinance_dremel.amer_weekly
        WHERE service_channel = 'LCS'
          AND region = 'Americas'
      GROUP BY ALL

),

Auction_Metrics AS
(
  SELECT
    source_sub_pod_id AS sub_pod_id,
    company_rollup.division_id AS division_id,
    SUM(IF($QTD AND $SEARCH, clicks, 0.0)) AS search_clicks_qtd,
    SUM(IF($QTD_LY AND $SEARCH, clicks, 0.0)) AS search_clicks_qtd_ly,
    SUM(IF($QTD AND $SEARCH, impressions, 0.0)) AS search_impressions_qtd,
    SUM(IF($QTD_LY AND $SEARCH, impressions, 0.0)) AS search_impressions_qtd_ly,

    SUM(IF($YTD AND $SEARCH, clicks, 0.0)) AS search_clicks_ytd,
    SUM(IF($YTD_LY AND $SEARCH, clicks, 0.0)) AS search_clicks_ytd_ly,
    SUM(IF($YTD AND $SEARCH, impressions, 0.0)) AS search_impressions_ytd,
    SUM(IF($YTD_LY AND $SEARCH, impressions, 0.0)) AS search_impressions_ytd_ly

FROM
  google.XP_DailyCurrentStats_F  
  WHERE
    service_channel = 'LCS'
    AND service_region = 'Americas'
    AND date >= '2024-01-01'
    AND billing_category = 'Billable'
GROUP BY ALL    
)

SELECT 
  R.*, 
  A.* EXCEPT (sub_pod_id, division_id)
  FROM Revenue AS R
  LEFT JOIN Auction_Metrics AS A USING (sub_pod_id, division_id)
;

CREATE OR REPLACE TABLE $FULL_TABLE_NAME 
  OPTIONS (need_dremel = FALSE)
  AS $QUERY
;
