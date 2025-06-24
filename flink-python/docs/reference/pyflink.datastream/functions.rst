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


=========
Functions
=========

RuntimeContext
--------------

.. currentmodule:: pyflink.datastream.functions

.. autosummary::
    :toctree: api/

    RuntimeContext.get_task_name
    RuntimeContext.get_number_of_parallel_subtasks
    RuntimeContext.get_max_number_of_parallel_subtasks
    RuntimeContext.get_index_of_this_subtask
    RuntimeContext.get_attempt_number
    RuntimeContext.get_task_name_with_subtasks
    RuntimeContext.get_job_parameter
    RuntimeContext.get_metrics_group
    RuntimeContext.get_state
    RuntimeContext.get_list_state
    RuntimeContext.get_map_state
    RuntimeContext.get_reducing_state
    RuntimeContext.get_aggregating_state


Function
--------

All user-defined functions.

.. currentmodule:: pyflink.datastream.functions

.. autosummary::
    :toctree: api/

    MapFunction
    CoMapFunction
    FlatMapFunction
    CoFlatMapFunction
    ReduceFunction
    AggregateFunction
    ProcessFunction
    KeyedProcessFunction
    CoProcessFunction
    KeyedCoProcessFunction
    WindowFunction
    AllWindowFunction
    ProcessWindowFunction
    ProcessAllWindowFunction
    KeySelector
    NullByteKeySelector
    FilterFunction
    Partitioner
    BroadcastProcessFunction
    KeyedBroadcastProcessFunction
