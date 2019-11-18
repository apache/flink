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
 define YEAR=random(1998,2002,uniform);
 define MONTH=random(11,12,uniform);
 define _LIMIT=100;
 
 [_LIMITA] select [_LIMITB] channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		 web_sales ws left outer join web_returns wr 
 			on (ws.ws_order_number = wr.wr_order_number and 
 			ws.ws_item_sk = wr.wr_item_sk)
                 ,date_dim
 		where 
 			wr.wr_return_amt > 10000 
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 2001
                         and d_moy = 12
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where 
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union
 select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select 
 		cs.cs_item_sk as item
 		,(cast(sum(coalesce(cr.cr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(cs.cs_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(cr.cr_return_amount,0)) as decimal(15,4))/
 		cast(sum(coalesce(cs.cs_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		catalog_sales cs left outer join catalog_returns cr
 			on (cs.cs_order_number = cr.cr_order_number and 
 			cs.cs_item_sk = cr.cr_item_sk)
                ,date_dim
 		where 
 			cr.cr_return_amount > 10000 
 			and cs.cs_net_profit > 1
                         and cs.cs_net_paid > 0
                         and cs.cs_quantity > 0
                         and cs_sold_date_sk = d_date_sk
                         and d_year = 2001
                         and d_moy = 12
                 group by cs.cs_item_sk
 	) in_cat
 ) catalog
 where 
 (
 catalog.return_rank <= 10
 or
 catalog.currency_rank <=10
 )
 union
 select 
 'store' as channel
 ,store.item
 ,store.return_ratio
 ,store.return_rank
 ,store.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select sts.ss_item_sk as item
 		,(cast(sum(coalesce(sr.sr_return_quantity,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(sr.sr_return_amt,0)) as decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		store_sales sts left outer join store_returns sr
 			on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
                ,date_dim
 		where 
 			sr.sr_return_amt > 10000 
 			and sts.ss_net_profit > 1
                         and sts.ss_net_paid > 0 
                         and sts.ss_quantity > 0
                         and ss_sold_date_sk = d_date_sk
                         and d_year = 2001
                         and d_moy = 12
 		group by sts.ss_item_sk
 	) in_store
 ) store
 where  (
 store.return_rank <= 10
 or 
 store.currency_rank <= 10
 )
 )
 order by 1,4,5,2
 [_LIMITC];
