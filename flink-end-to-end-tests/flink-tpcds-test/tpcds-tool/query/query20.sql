-- start query 1 in stream 0 using template ../query_templates_qualified/query20.tpl
select  i_item_id
       ,i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in ('Sports', 'Books', 'Home')
   and cs_sold_date_sk = d_date_sk
 and d_date between cast('1999-02-22' as date)
 				and (cast('1999-02-22' as date) + INTERVAL '30' day)
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
limit 100

-- end query 1 in stream 0 using template ../query_templates_qualified/query20.tpl
