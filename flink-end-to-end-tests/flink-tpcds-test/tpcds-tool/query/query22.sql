-- start query 1 in stream 0 using template ../query_templates_qualified/query22.tpl
select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(cast(inv_quantity_on_hand as decimal(7, 2))) qoh
       from inventory
           ,date_dim
           ,item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1200 and 1200 + 11
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
limit 100

-- end query 1 in stream 0 using template ../query_templates_qualified/query22.tpl
