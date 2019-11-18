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
 define SALES_DATE=date([YEAR]+"-08-01",[YEAR]+"-08-30",sales);
 define _LIMIT=100;
 
 with ss as
 (select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from store_sales,
      date_dim,
      store
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date) 
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
       and ss_store_sk = s_store_sk
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         sum(sr_return_amt) as `returns`,
         sum(sr_net_loss) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
       and sr_store_sk = s_store_sk
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from catalog_sales,
      date_dim
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         sum(cr_return_amount) as `returns`,
         sum(cr_net_loss) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
 group by cr_call_center_sk
 ), 
 ws as
 ( select wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit
 from web_sales,
      date_dim,
      web_page
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
       and ws_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk), 
 wr as
 (select wp_web_page_sk,
        sum(wr_return_amt) as `returns`,
        sum(wr_net_loss) as profit_loss
 from web_returns,
      date_dim,
      web_page
 where wr_returned_date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and (cast('2000-08-23' as date) + INTERVAL '30' day)
       and wr_web_page_sk = wp_web_page_sk
 group by wp_web_page_sk)
 [_LIMITA] select [_LIMITB] channel
        , id
        , sum(sales) as sales
        , sum(`returns`) as `returns`
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(`returns`, 0) as `returns`
        , (profit - coalesce(profit_loss,0)) as profit
 from   ss left join sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , `returns`
        , (profit - profit_loss) as profit
 from  cs
       , cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(`returns`, 0) `returns`
        , (profit - coalesce(profit_loss,0)) as profit
 from   ws left join wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 [_LIMITC];
 

