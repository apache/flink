.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################

Quickstart
==========

This quickstart guide will help you get up and running with PyFlink in just a few minutes. You'll learn how to:

1. Set up a PyFlink environment
2. Create a simple streaming job
3. Run your first PyFlink application

Prerequisites
-------------

Before you begin, make sure you have:

- Python 3.9, 3.10, 3.11, or 3.12 installed
- PyFlink installed (see :doc:`installation`)

Your First PyFlink Application
------------------------------

Let's create a simple streaming application that reads from a socket and counts words in real-time.

Create a file named ``word_count.py`` with the following content:

.. code-block:: python

   from pyflink.datastream import StreamExecutionEnvironment
   from pyflink.table import StreamTableEnvironment, EnvironmentSettings

   def word_count_streaming():
       # Create a streaming execution environment
       env = StreamExecutionEnvironment.get_execution_environment()

       # Create a table environment
       settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
       table_env = StreamTableEnvironment.create(env, settings)

       # Create a source table from socket
       source_ddl = """
           CREATE TABLE source (
               word STRING
           ) WITH (
               'connector' = 'socket',
               'hostname' = 'localhost',
               'port' = '9999',
               'format' = 'csv'
           )
       """

       # Create a sink table to print results
       sink_ddl = """
           CREATE TABLE sink (
               word STRING,
               count BIGINT
           ) WITH (
               'connector' = 'print'
           )
       """

       # Execute DDL statements
       table_env.execute_sql(source_ddl)
       table_env.execute_sql(sink_ddl)

       # Create a query
       table_env.sql_query("""
           SELECT word, COUNT(*) as count
           FROM source
           GROUP BY word
       """).execute_insert('sink')

   if __name__ == '__main__':
       word_count_streaming()

Running the Application
-----------------------

1. **Start a socket server** (in a separate terminal):

   .. code-block:: bash

      nc -lk 9999

2. **Run your PyFlink application**:

   .. code-block:: bash

      python word_count.py

3. **Send some text** to the socket (in the nc terminal):

   .. code-block:: text

      hello world
      hello flink
      world of streaming

You should see the word count results printed in your PyFlink application output.

What's Next?
------------

Now that you've run your first PyFlink application, you can explore:

- :doc:`../user_guide/datastream_tutorial` - Learn about the DataStream API
- :doc:`../user_guide/table_api_tutorial` - Learn about the Table API & SQL
- :doc:`../examples/index` - Browse complete examples
- :doc:`../cookbook/index` - Find practical recipes for common tasks

For more complex examples and tutorials, check out the :doc:`../examples/index` section.
