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

=====
Model
=====

A model registered in the catalog can be retrieved with
:meth:`~pyflink.table.TableEnvironment.from_model`. Use
:meth:`~pyflink.table.Model.predict` to apply it to a table, optionally passing
runtime options such as asynchronous execution settings.

.. currentmodule:: pyflink.table

.. autosummary::
    :toctree: api/

    Model
    Model.predict
    Model.get_resolved_input_schema
    Model.get_resolved_output_schema
