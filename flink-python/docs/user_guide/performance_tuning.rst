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

Performance Tuning
==================

This section covers performance optimization techniques for PyFlink applications.

Key Factors
-----------

Several factors affect PyFlink application performance:

* **Parallelism**: Number of parallel instances for operators
* **Memory Configuration**: Heap and off-heap memory settings
* **State Backend**: Choice of state storage backend
* **Network Buffers**: Network buffer configuration
* **Checkpointing**: Checkpoint interval and timeout settings

Parallelism Configuration
-------------------------

.. code-block:: python

   from pyflink.datastream import StreamExecutionEnvironment
   from pyflink.table import StreamTableEnvironment, EnvironmentSettings

   # Create execution environment
   env = StreamExecutionEnvironment.get_execution_environment()

   # Set global parallelism
   env.set_parallelism(4)

   # Set parallelism for specific operators
   ds = env.from_collection([1, 2, 3, 4, 5])
   ds = ds.map(lambda x: x * 2).set_parallelism(2)
   ds = ds.filter(lambda x: x > 5).set_parallelism(1)
