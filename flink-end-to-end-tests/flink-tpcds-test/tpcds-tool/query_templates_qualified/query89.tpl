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
define YEAR = random(1998, 2002, uniform);
define IDX = ulist(random(1, rowcount("categories"), uniform), 6);
define CAT_A = distmember(categories, [IDX.1], 1);
define CLASS_A = DIST(distmember(categories, [IDX.1], 2), 1, 1);
define CAT_B = distmember(categories, [IDX.2], 1);
define CLASS_B = DIST(distmember(categories, [IDX.2], 2), 1, 1);
define CAT_C = distmember(categories, [IDX.3], 1);
define CLASS_C = DIST(distmember(categories, [IDX.3], 2), 1, 1);
define CAT_D = distmember(categories, [IDX.4], 1);
define CLASS_D = DIST(distmember(categories, [IDX.4], 2), 1, 1);
define CAT_E = distmember(categories, [IDX.5], 1);
define CLASS_E = DIST(distmember(categories, [IDX.5], 2), 1, 1);
define CAT_F = distmember(categories, [IDX.6], 1);
define CLASS_F = DIST(distmember(categories, [IDX.6], 2), 1, 1);
define _LIMIT=100;

[_LIMITA] select [_LIMITB] *
from(
select i_category, i_class, i_brand,
       s_store_name, s_company_name,
       d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (1999) and
        ((i_category in ('Books','Electronics','Sports') and
          i_class in ('computers','stereo','football')
         )
      or (i_category in ('Men','Jewelry','Women') and
          i_class in ('shirts','birdal','dresses') 
        ))
group by i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
[_LIMITC];
