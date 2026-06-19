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

State Management and Fault Tolerance
=====================================

This section covers state management and fault tolerance in PyFlink applications.

.. note::
   This documentation is under development. For now, please refer to the
   :doc:`DataStream API Reference </reference/pyflink.datastream/index>` for detailed state management documentation.

State Types
-----------

State is a fundamental concept in stream processing that allows you to maintain data across multiple
events. PyFlink provides several types of state:

* **Keyed State**: State that is scoped to a specific key
* **Operator State**: State that is scoped to an operator instance
* **Raw State**: Low-level state access for advanced use cases

Keyed State Example
-------------------

.. code-block:: python

   from pyflink.common import Types
   from pyflink.datastream import StreamExecutionEnvironment
   from pyflink.datastream.functions import RuntimeContext, MapFunction
   from pyflink.datastream.state import ValueStateDescriptor

   class CountFunction(MapFunction):
       def open(self, runtime_context: RuntimeContext):
           # Define state descriptor
           state_desc = ValueStateDescriptor('count', Types.LONG())
           self.count_state = runtime_context.get_state(state_desc)

       def map(self, value):
           # Get current count
           current_count = self.count_state.value()
           if current_count is None:
               current_count = 0

           # Increment count
           current_count += 1
           self.count_state.update(current_count)

           return value, current_count

   # Usage
   env = StreamExecutionEnvironment.get_execution_environment()
   ds = env.from_collection([1, 2, 3, 1, 2, 1])
   ds = ds.map(lambda x: (x, 1)) \
          .key_by(lambda x: x[0]) \
          .map(CountFunction())
   ds.print()
