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
 define YEAR= random(1998, 2000, uniform); 
 define MONTH = random(1,7,uniform); 
 define TOPPERCENT=random(95,95,uniform); 
 define _LIMIT=100;
 
 with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (50/100.0) * (select
  *
 from max_store_sales))
 [_LIMITA] select [_LIMITB] c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        from catalog_sales
            ,customer
            ,date_dim 
        where d_year = 2000 
         and d_moy = 2 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk 
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       from web_sales
           ,customer
           ,date_dim 
       where d_year = 2000 
         and d_moy = 2 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name) 
     order by c_last_name,c_first_name,sales
  [_LIMITC];
