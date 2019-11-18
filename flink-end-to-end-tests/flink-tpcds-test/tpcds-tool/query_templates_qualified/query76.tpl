--
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- 
define NULLCOLCS=text({"cs_bill_customer_sk",1},{"cs_bill_hdemo_sk",1},{"cs_bill_addr_sk",1},{"cs_ship_customer_sk",1},{"cs_ship_cdemo_sk",1},{"cs_ship_hdemo_sk",1},{"cs_ship_addr_sk",1},{"cs_ship_mode_sk",1},{"cs_warehouse_sk",1},{"cs_promo_sk",1});
define NULLCOLSS= text({"ss_customer_sk",1},{"ss_cdemo_sk",1},{"ss_hdemo_sk",1},{"ss_addr_sk",1},{"ss_store_sk",1},{"ss_promo_sk",1});
define NULLCOLWS=text({"ws_bill_customer_sk",1},{"ws_bill_hdemo_sk",1},{"ws_bill_addr_sk",1},{"ws_ship_customer_sk",1},{"ws_ship_cdemo_sk",1},{"ws_ship_hdemo_sk",1},{"ws_ship_addr_sk",1},{"ws_web_page_sk",1},{"ws_web_site_sk",1},{"ws_ship_mode_sk",1},{"ws_warehouse_sk",1},{"ws_promo_sk",1});
define _LIMIT=100;

[_LIMITA] select [_LIMITB] channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, 'ss_store_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales, item, date_dim
         WHERE ss_store_sk IS NULL
           AND ss_sold_date_sk=d_date_sk
           AND ss_item_sk=i_item_sk
        UNION ALL
        SELECT 'web' as channel, 'ws_ship_customer_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales, item, date_dim
         WHERE ws_ship_customer_sk IS NULL
           AND ws_sold_date_sk=d_date_sk
           AND ws_item_sk=i_item_sk
        UNION ALL
        SELECT 'catalog' as channel, 'cs_ship_addr_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales, item, date_dim
         WHERE cs_ship_addr_sk IS NULL
           AND cs_sold_date_sk=d_date_sk
           AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
[_LIMITC];

 
 

