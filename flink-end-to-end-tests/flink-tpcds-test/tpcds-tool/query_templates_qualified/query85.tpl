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
 define MS= ulist(dist(marital_status, 1, 1), 3);
 define ES= ulist(dist(education, 1, 1), 3);
 define STATE= ulist(dist(fips_county, 3, 1), 9);
 define YEAR= random(1998,2002, uniform);
 define _LIMIT=100;

 [_LIMITA] select [_LIMITB] substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2000
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'College'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'W'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '2 yr Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('IN', 'OH', 'NJ')
     and ws_net_profit between 100 and 200  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WI', 'CT', 'KY')
     and ws_net_profit between 150 and 300  
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('LA', 'IA', 'AR')
     and ws_net_profit between 50 and 250  
    )
   )
group by r_reason_desc
order by substr(r_reason_desc,1,20)
        ,avg(ws_quantity)
        ,avg(wr_refunded_cash)
        ,avg(wr_fee)
[_LIMITC]; 
