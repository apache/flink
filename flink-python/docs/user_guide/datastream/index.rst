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

==============
DataStream API
==============

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially
created from various sources (e.g., message queues, socket streams, files). Results are returned via
sinks, which may for example write the data to files, or to standard output (for example the command
line terminal).

Python DataStream API is a Python version of DataStream API which allows Python users to write
Python DataStream API jobs.

.. toctree::
    :maxdepth: 2

    data_types
    operators
    process_function
