---
title: "Vector Search"
weight: 7
type: docs
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

# Vector Search

{{< label Batch >}} {{< label Streaming >}}

Flink SQL provides the `VECTOR_SEARCH` table-valued function (TVF) to perform a vector search in SQL queries. This function allows you to search similar rows according to the high-dimension vectors.

## VECTOR_SEARCH Function

The `VECTOR_SEARCH` uses a processing-time attribute to correlate rows to the latest version of data in an external table. It's very similar to a lookup join in Flink SQL, however, the difference is 
`VECTOR_SEARCH` uses the input data vector to compare the similarity with data in the external table and return the top-k most similar rows.

### Syntax

```sql
SELECT * 
FROM input_table, LATERAL TABLE(VECTOR_SEARCH(
   TABLE vector_table, 
   input_table.vector_column, 
   DESCRIPTOR(index_column),
   top_k,
   [CONFIG => MAP['key', 'value']]
   ))
```

### Parameters

- `input_table`: The input table containing the data to be processed
- `vector_table`: The name of external table that allows searching via vector
- `vector_column`: The name of the column in the input table, its type should be FLOAT ARRAY or DOUBLE ARRAY
- `index_column`: A descriptor specifying which column from the vector table should be used to compare the similarity with the input data
- `top_k`: The number of top-k most similar rows to return
- `config`: (Optional) A map of configuration options for the vector search

### Configuration Options

The following configuration options can be specified in the config map:

{{< generated/vector_search_runtime_config_configuration >}}

### Example

```sql
-- Basic usage
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  input_table.vector_column,
  DESCRIPTOR(index_column),
  10
));

-- With configuration options
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  input_table.vector_column,
  DESCRIPTOR(index_column),
  10,
  MAP['async', 'true', 'timeout', '100s']
));

-- Using named parameters
SELECT * FROM 
input_table, LATERAL TABLE(VECTOR_SEARCH(
  SEARCH_TABLE => TABLE vector_table,
  COLUMN_TO_QUERY => input_table.vector_column,
  COLUMN_TO_SEARCH => DESCRIPTOR(index_column),
  TOP_K => 10,
  CONFIG => MAP['async', 'true', 'timeout', '100s']
));

-- Searching with contant value
SELECT * 
FROM TABLE(VECTOR_SEARCH(
  TABLE vector_table,
  ARRAY[10, 20],
  DESCRIPTOR(index_column),
  10,
));
```

### Output

The output table contains all columns from the input table, the vector search table columns and a column named `score` to indicate the similarity between the input row and matched row.

### Notes

1. The implementation of the vector table must implement interface `org.apache.flink.table.connector.source.VectorSearchTableSource`. Please refer to [Vector Search Table Source]({{< ref "/docs/dev/table/sourcesSinks" >}}#vector-search-table-source) for details.
2. `VECTOR_SEARCH` only supports to consume append-only tables.
3. `VECTOR_SEARCH` does not require the `LATERAL` keyword when the function call has no correlation with other tables. For example, if the search column is a constant or literal value, `LATERAL` can be omitted.

{{< top >}} 
