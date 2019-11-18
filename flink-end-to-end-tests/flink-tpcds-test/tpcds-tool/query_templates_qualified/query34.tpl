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
 define BPONE= text({"1001-5000",1},{">10000",1},{"501-1000",1});
 define BPTWO= text({"0-500",1},{"Unknown",1},{"5001-10000",1});
 define YEAR= random(1998, 2000, uniform);
 define COUNTYNUMBER=ulist(random(1, rowcount("active_counties", "store"), uniform), 8);
 define COUNTY_A=distmember(fips_county, [COUNTYNUMBER.1], 2);
 define COUNTY_B=distmember(fips_county, [COUNTYNUMBER.2], 2);
 define COUNTY_C=distmember(fips_county, [COUNTYNUMBER.3], 2);
 define COUNTY_D=distmember(fips_county, [COUNTYNUMBER.4], 2);
 define COUNTY_E=distmember(fips_county, [COUNTYNUMBER.5], 2);
 define COUNTY_F=distmember(fips_county, [COUNTYNUMBER.6], 2);
 define COUNTY_G=distmember(fips_county, [COUNTYNUMBER.7], 2);
 define COUNTY_H=distmember(fips_county, [COUNTYNUMBER.8], 2);
 
 
 select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '>10000' or
         household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0 
	then cast(household_demographics.hd_dep_count as decimal(7, 2))/ cast(household_demographics.hd_vehicle_count as decimal(7, 2))
	else null 
	end)  > 1.2
    and date_dim.d_year in (1999,1999+1,1999+2)
    and store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                           'Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dn,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;

