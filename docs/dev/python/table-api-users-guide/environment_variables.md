---
title: "Environment Variables"
nav-parent_id: python_tableapi
nav-pos: 140
---
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

These environment variables will change the behavior of PyFlink:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Environment Variable</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <strong>FLINK_HOME</strong>
      </td>
      <td>
        PyFlink needs a Java process as the Py4J gateway server.
        Normally the Java process is started from the bundled flink jars under the PyFlink installation folder.
        If this environment variable is set, the Java process will be started from the flink jars under the $FLINK_HOME folder.
      </td>
    </tr>
    <tr>
      <td>
        <strong>PYFLINK_CLIENT_EXECUTABLE</strong>
      </td>
      <td>
        The path of the python interpreter used to launch the python process when compiling the jobs containing Python UDFs.
        Equivalent to the configuration option 'python.client.executable'. The priority is as following: 
        <ol>
        <li>the configuration 'python.client.executable' defined in the source code; </li>
        <li>the environment variable PYFLINK_CLIENT_EXECUTABLE; </li>
        <li>the configuration 'python.client.executable' defined in flink-conf.yaml</li>
        </ol>
        If none of above is set, the default python interpreter 'python' will be used.
      </td>
    </tr>
  </tbody>
</table>
