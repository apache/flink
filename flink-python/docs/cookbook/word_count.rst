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

Word Count
==========

This recipe demonstrates how to implement a word count application using PyFlink. This is a classic example
that shows the basic concepts of stream processing.

Problem
-------

Count the frequency of each word in a stream of text data.

Solution
--------

We'll implement this using both the DataStream API and Table API approaches.

DataStream API Approach
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyflink.common import Types
   from pyflink.datastream import StreamExecutionEnvironment
   from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
   from pyflink.common.serialization import Encoder

   def word_count_datastream():
       # Create execution environment
       env = StreamExecutionEnvironment.get_execution_environment()

       # Create source from socket
       text_stream = env.socket_text_stream("localhost", 9999)

       # Transform: split text into words and count them
       word_counts = text_stream \
           .flat_map(lambda line: line.lower().split()) \
           .map(lambda word: (word, 1)) \
           .key_by(lambda x: x[0]) \
           .reduce(lambda x, y: (x[0], x[1] + y[1]))

       # Sink: print results
       word_counts.print()

       # Execute
       env.execute("Word Count DataStream")

   if __name__ == '__main__':
       word_count_datastream()

Table API Approach
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyflink.table import EnvironmentSettings, TableEnvironment
   from pyflink.table.expressions import col, call

   def word_count_table():
       # Create table environment
       env_settings = EnvironmentSettings.in_streaming_mode()
       table_env = TableEnvironment.create(env_settings)

       # Create source table
       table_env.execute_sql("""
           CREATE TABLE text_source (
               line STRING
           ) WITH (
               'connector' = 'socket',
               'hostname' = 'localhost',
               'port' = '9999',
               'format' = 'csv'
           )
       """)

       # Create sink table
       table_env.execute_sql("""
           CREATE TABLE word_counts (
               word STRING,
               count BIGINT
           ) WITH (
               'connector' = 'print'
           )
       """)

       # Query: split text and count words
       source_table = table_env.from_path("text_source")

       # Use a UDF to split text into words
       @udf(result_type=DataTypes.ARRAY(DataTypes.STRING()))
       def split_words(text):
           return text.lower().split()

       table_env.create_temporary_function("split_words", split_words)

       # Execute word count query
       result = source_table \
           .select(call("split_words", col("line")).alias("words")) \
           .flat_map(lambda x: x.words) \
           .group_by(col("words")) \
           .select(col("words").alias("word"), call("count", "*").alias("count"))

       # Insert results
       result.execute_insert("word_counts").wait()

   if __name__ == '__main__':
       word_count_table()

Running the Example
-------------------

1. **Start a socket server** (in a separate terminal):

   .. code-block:: bash

      nc -lk 9999

2. **Run the PyFlink application**:

   .. code-block:: bash

      python word_count.py

3. **Send text to the socket**:

   .. code-block:: text

      hello world
      hello flink
      world of streaming

Expected Output
---------------

You should see output similar to:

.. code-block:: text

   (hello, 2)
   (world, 2)
   (flink, 1)
   (of, 1)
   (streaming, 1)

Variations
----------

* **File-based word count**: Replace socket source with file source
* **Windowed word count**: Add time windows to get word counts over time periods
* **Case-insensitive counting**: Normalize words to lowercase before counting
* **Filtering**: Exclude common stop words or short words

This recipe demonstrates key PyFlink concepts:
- Stream processing with unbounded data
- Keyed operations for grouping
- Aggregation with reduce operations
- Multiple API approaches (DataStream vs Table API)
