-- start query 12 in stream 0 using template query12.tpl
SELECT  i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
FROM
	  web_sales, item, date_dim
where 
	ws_item_sk = i_item_sk 
  	and i_category in ('Electronics', 'Women', 'Men')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('1998-01-02' as date) 
				and (cast('1998-01-02' as date) + INTERVAL '30' day)
GROUP BY
  i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY
  i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100

-- end query 12 in stream 0 using template query12.tpl
