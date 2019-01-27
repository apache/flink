-- start query 82 in stream 0 using template query82.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 0 and 0+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1998-01-02' as date) and (cast('1998-01-02' as date) +  INTERVAL '60' day)
 and i_manufact_id in (1,2,3,4)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100

-- end query 82 in stream 0 using template query82.tpl
