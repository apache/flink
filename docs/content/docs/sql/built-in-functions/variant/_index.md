---
title: "Variant"
weight: 17
type: docs
bookCollapseSection: true
sqlFunctions: variant
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

# Variant Functions

Variant functions parse JSON and XML text into [`VARIANT`]({{< ref "docs/sql/reference/data-types" >}}#variant) values and read values out of them, such as `PARSE_JSON`, `PARSE_XML` and `VARIANT_GET`. To turn a `VARIANT` back into JSON text, use [`JSON_STRING`]({{< ref "docs/sql/built-in-functions/json" >}}).

[XML and VARIANT]({{< ref "docs/sql/built-in-functions/variant/xml" >}}) describes how `PARSE_XML` and `XML_STRING` map XML to a `VARIANT` and back, and [VARIANT Paths]({{< ref "docs/sql/built-in-functions/variant/paths" >}}) describes the paths that `VARIANT_GET` and `TRY_VARIANT_GET` take.

{{< sql_functions "variant" >}}
