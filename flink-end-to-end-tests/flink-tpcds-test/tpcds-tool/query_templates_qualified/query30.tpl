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
 define STATE= dist(fips_county, 3, 1);
 define YEAR= random(1999, 2002, uniform);
 define _LIMIT=100;
 
 with customer_total_return as
 (select wr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(wr_return_amt) as ctr_total_return
 from web_returns
     ,date_dim
     ,customer_address
 where wr_returned_date_sk = d_date_sk 
   and d_year =2002
   and wr_returning_addr_sk = ca_address_sk 
 group by wr_returning_customer_sk
         ,ca_state)
 [_LIMITA] select [_LIMITB] c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date_sk,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'GA'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                  ,c_last_review_date_sk,ctr_total_return
[_LIMITC];
