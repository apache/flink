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

 select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2000
 and
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'M'
   and
   cd_education_status = '4 yr Degree'
   and
   ss_sales_price between 100.00 and 150.00
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'D'
   and
   cd_education_status = '2 yr Degree'
   and
   ss_sales_price between 50.00 and 100.00
  )
 or
 (
  cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'S'
   and
   cd_education_status = 'College'
   and
   ss_sales_price between 150.00 and 200.00
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('CO', 'OH', 'TX')
  and ss_net_profit between 0 and 2000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('OR', 'MN', 'KY')
  and ss_net_profit between 150 and 3000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('VA', 'CA', 'MS')
  and ss_net_profit between 50 and 25000
  )
 )
;

