-- Test ALTER VIEW SET WATERMARK syntax
-- This file demonstrates the new ALTER VIEW SET WATERMARK feature

-- Example 1: Set watermark on event_time column with 5 second delay
ALTER VIEW my_view SET WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND;

-- Example 2: Set watermark on ts column with bounded out-of-orderness
ALTER VIEW user_behavior_view SET WATERMARK FOR ts AS ts - INTERVAL '10' SECOND;

-- Example 3: Set watermark with millisecond precision
ALTER VIEW orders_view SET WATERMARK FOR order_time AS order_time - INTERVAL '100' MILLISECOND;

-- Example 4: Set watermark on a view with qualified name
ALTER VIEW my_catalog.my_database.my_view SET WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE;
