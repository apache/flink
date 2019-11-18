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

define CINDX = random(1,rowcount("categories"),uniform);
define CATEGORY = distmember(categories,[CINDX],1);
define YEAR = random(1999,2002,uniform);
define _LIMIT=100;

WITH all_sales AS (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk
                          JOIN date_dim ON d_date_sk=cs_sold_date_sk
                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number 
                                                    AND cs_item_sk=cr_item_sk)
       WHERE i_category='Books'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
       FROM store_sales JOIN item ON i_item_sk=ss_item_sk
                        JOIN date_dim ON d_date_sk=ss_sold_date_sk
                        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number 
                                                AND ss_item_sk=sr_item_sk)
       WHERE i_category='Books'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
             ,ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
       FROM web_sales JOIN item ON i_item_sk=ws_item_sk
                      JOIN date_dim ON d_date_sk=ws_sold_date_sk
                      LEFT JOIN web_returns ON (ws_order_number=wr_order_number 
                                            AND ws_item_sk=wr_item_sk)
       WHERE i_category='Books') sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id)
[_LIMITA] SELECT [_LIMITB] prev_yr.d_year AS prev_year
                          ,curr_yr.d_year AS `year`
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM all_sales curr_yr, all_sales prev_yr
 WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
   AND curr_yr.i_class_id=prev_yr.i_class_id
   AND curr_yr.i_category_id=prev_yr.i_category_id
   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
   AND curr_yr.d_year=2002
   AND prev_yr.d_year=2002-1
   AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
 ORDER BY sales_cnt_diff,sales_amt_diff
 [_LIMITC];
