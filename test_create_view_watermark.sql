-- Test CREATE VIEW with WATERMARK functionality

-- Test 1: Basic CREATE VIEW with WATERMARK
CREATE VIEW my_view
WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
AS SELECT 
    event_id,
    event_time,
    user_id,
    event_type
FROM source_table;

-- Test 2: CREATE VIEW with field list and WATERMARK
CREATE VIEW order_view (order_id, order_time, customer_id, amount)
COMMENT 'Orders view with watermark'
WATERMARK FOR order_time AS order_time - INTERVAL '10' SECOND
AS SELECT 
    id,
    order_timestamp,
    customer,
    total
FROM orders_table;

-- Test 3: CREATE TEMPORARY VIEW with WATERMARK
CREATE TEMPORARY VIEW temp_view
WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE
AS SELECT 
    transaction_id,
    ts,
    amount
FROM transactions;

-- Test 4: CREATE VIEW IF NOT EXISTS with WATERMARK  
CREATE VIEW IF NOT EXISTS catalog1.db1.event_view
WATERMARK FOR event_time AS event_time - INTERVAL '100' MILLISECOND
AS SELECT 
    event_id,
    event_time,
    data
FROM catalog1.db1.events;

-- Test 5: CREATE VIEW with complex watermark expression
CREATE VIEW complex_watermark_view
WATERMARK FOR process_time AS process_time - INTERVAL '5' SECOND - INTERVAL '100' MILLISECOND
AS SELECT 
    id,
    process_time,
    value
FROM source;

-- Test 6: CREATE VIEW without WATERMARK (backward compatibility)
CREATE VIEW simple_view
AS SELECT 
    id,
    name,
    value
FROM base_table;

-- Test 7: CREATE VIEW with COMMENT and WATERMARK
CREATE VIEW commented_view
COMMENT 'This is a view with watermark for event time processing'
WATERMARK FOR event_ts AS event_ts - INTERVAL '3' SECOND  
AS SELECT 
    event_id,
    event_ts,
    payload
FROM events;

-- Test 8: Fully qualified view name with WATERMARK
CREATE VIEW my_catalog.my_database.my_view
WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND
AS SELECT 
    id,
    rowtime,
    data
FROM my_catalog.my_database.source_table;

-- Expected behavior:
-- 1. Parser should successfully parse all CREATE VIEW statements with WATERMARK clause
-- 2. WATERMARK clause should be optional (backward compatible)
-- 3. WATERMARK should be specified between COMMENT and AS clauses
-- 4. Watermark expression should support INTERVAL operations
-- 5. View schema should include watermark specification
