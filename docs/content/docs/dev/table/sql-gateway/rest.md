---
title: REST Endpoint
weight: 2
type: docs
aliases:
- /dev/table/sql-gateway/rest.html
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

# REST Endpoint

The REST endpoint allows user to connect to SQL Gateway with REST API.

Overview of SQL Processing
----------------

### Open Session

When the client connects to the SQL Gateway, the SQL Gateway creates a `Session` as the context to store the users-specified information 
during the interactions between the client and SQL Gateway. After the creation of the `Session`, the SQL Gateway server returns an identifier named
`SessionHandle` for later interactions.

### Submit SQL

After the registration of the `Session`, the client can submit the SQL to the SQL Gateway server. When submitting the SQL,
the SQL is translated to an `Operation` and an identifier named `OperationHandle` is returned for fetch results later. The Operation has
its lifecycle, the client is able to cancel the execution of the `Operation` or close the `Operation` to release the resources used by the `Operation`.

### Fetch Results

With the `OperationHandle`, the client can fetch the results from the `Operation`. If the `Operation` is ready, the SQL Gateway will return a batch 
of the data with the corresponding schema and a URI that is used to fetch the next batch of the data. When all results have been fetched, the 
SQL Gateway will fill the `resultType` in the response with value `EOS` and the URI to the next batch of the data is null.

{{< img width="100%" src="/fig/sql-gateway-interactions.png" alt="SQL Gateway Interactions" >}}

Endpoint Options
----------------

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.address</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The address that should be used by clients to connect to the sql gateway server.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.bind-address</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The address that the sql gateway server binds itself.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.bind-port</h5></td>
            <td style="word-wrap: break-word;">"8083"</td>
            <td>String</td>
            <td>The port that the sql gateway server binds itself. Accepts a list of ports (“50100,50101”), ranges (“50100-50200”) or a combination of both. It is recommended to set a range of ports to avoid collisions when multiple sql gateway servers are running on the same machine.</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.port</h5></td>
            <td style="word-wrap: break-word;">8083</td>
            <td>Integer</td>
            <td>The port that the client connects to. If bind-port has not been specified, then the sql gateway server will bind to this port.</td>
        </tr>
    </tbody>
</table>

REST API
----------------

The available OpenAPI specification is as follows. The default version is v2.

| Version                       | Description |
| ----------- | ------- |
| [Open API v1 specification]({{< ref_static "generated/rest_v1_sql_gateway.yml" >}}) | Allow users to submit statements to the gateway and execute. |
| [Open API v2 specification]({{< ref_static "generated/rest_v2_sql_gateway.yml" >}}) | Supports SQL Client to connect to the gateway. |

{{< hint warning >}}
The OpenAPI specification is still experimental.
{{< /hint >}}

#### API reference

{{< tabs "f00ed142-b05f-44f0-bafc-799080c1d40d" >}}
{{< tab "v2" >}}

{{< generated/rest_v2_sql_gateway >}}

{{< /tab >}}
{{< tab "v1" >}}

{{< generated/rest_v1_sql_gateway >}}

{{< /tab >}}
{{< /tabs >}}

Data Type Mapping
----------------

Currently, REST endpoint supports to serialize the `RowData` with query parameter `rowFormat`. REST endpoint uses JSON format to serialize 
the Table Objects. Please refer [JSON format]({{< ref "docs/connectors/table/formats/json#data-type-mapping" >}}) to the mappings. 

REST endpoint also supports to serialize the `RowData` with `PLAIN_TEXT` format that automatically cast all columns to the `String`.

{{< top >}}
