.. raw:: html

   <!--
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
   -->

Overview
==========

.. image:: /assets/fig/pyflink.svg
   :alt: PyFlink
   :class: offset
   :width: 50%

PyFlink is a Python API for Apache Flink that allows you to build
scalable batch and streaming workloads, such as real-time data
processing pipelines, large-scale exploratory data analysis, Machine
Learning (ML) pipelines and ETL processes. If you're already familiar
with Python and libraries such as Pandas, then PyFlink makes it simpler
to leverage the full capabilities of the Flink ecosystem. Depending on
the level of abstraction you need, there are two different APIs that can
be used in PyFlink:

- The **PyFlink Table API** allows you to write powerful relational
  queries in a way that is similar to using SQL or working with tabular
  data in Python.
- At the same time, the **PyFlink DataStream API** gives you lower-level
  control over the core building blocks of Flink, `state <https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/>`_ and `time <https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/>`_, to build more complex stream processing use
  cases.

.. raw:: html

   <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin: 2rem 0;">
     <div>
       <h3>Try PyFlink</h3>
       <p>If you're interested in playing around with Flink, try one of our tutorials:</p>
       <ul>
         <li><a href="datastream_tutorial.html">Intro to PyFlink DataStream API</a></li>
         <li><a href="table_api_tutorial.html">Intro to PyFlink Table API</a></li>
       </ul>
     </div>
     <div>
       <h3>Explore PyFlink</h3>
       <p>The reference documentation covers all the details. Some starting points:</p>
       <ul>
         <li><a href="datastream/index.html">PyFlink DataStream API</a></li>
         <li><a href="table/index.html">PyFlink Table API & SQL</a></li>
       </ul>
     </div>
   </div>

For more examples, you can also refer to `PyFlink Examples <https://github.com/apache/flink/tree/master/flink-python/pyflink/examples>`_.

Get Help with PyFlink
~~~~~~~~~~~~~~~~~~~~~

If you get stuck, check out our `community support
resources <https://flink.apache.org/community.html>`__. In particular,
Apache Flink's user mailing list is consistently ranked as one of the
most active of any Apache project, and is a great way to get help
quickly.
