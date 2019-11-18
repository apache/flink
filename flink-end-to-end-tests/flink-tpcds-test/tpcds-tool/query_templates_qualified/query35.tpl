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

 define YEAR=random(1999,2002,uniform);
 define AGGONE= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define AGGTWO= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define AGGTHREE= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define _LIMIT=100;
 
 [_LIMITA] select [_LIMITB]  
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  min(cd_dep_count),
  max(cd_dep_count),
  avg(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  min(cd_dep_employed_count),
  max(cd_dep_employed_count),
  avg(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  min(cd_dep_college_count),
  max(cd_dep_college_count),
  avg(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2002 and
                d_qoy < 4) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_qoy < 4) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_qoy < 4))
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 [_LIMITC];
 

