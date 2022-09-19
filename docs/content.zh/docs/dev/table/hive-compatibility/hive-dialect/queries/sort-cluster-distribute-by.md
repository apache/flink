---
title: "Sort/Cluster/Distributed By"
weight: 2
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

# Sort/Cluster/Distributed by Clause

## Sort By

### Description

Unlike [ORDER BY]({{< ref "docs/dev/table/hive-compatibility/hive-dialect/queries/overview" >}}#order-by-clause) which guarantees a total order of output,
`SORT BY` only guarantees the result rows with each partition is in the user specified order.
So when there's more than one partition, `SORT BY` may return result that's partially ordered.

### Syntax

```sql
query: SELECT expression [ , ... ] FROM src sortBy
sortBy: SORT BY expression colOrder [ , ... ]
colOrder: ( ASC | DESC )
```

### Parameters
- colOrder

  it's used specified the order of returned rows. The default order is `ASC`.

### Examples

```sql
SELECT x, y FROM t SORT BY x;
SELECT x, y FROM t SORT BY abs(y) DESC;
```

## Distribute By

### Description

The `DISTRIBUTE BY` clause is used to repartition the data.
The data with same value evaluated by the specified expression will be in same partition.

### Syntax

```sql
distributeBy: DISTRIBUTE BY expression [ , ... ]
query: SELECT expression [ , ... ] FROM src distributeBy
```

### Examples

```sql
-- only use DISTRIBUTE BY clause
SELECT x, y FROM t DISTRIBUTE BY x;
SELECT x, y FROM t DISTRIBUTE BY abs(y);

-- use both DISTRIBUTE BY and SORT BY clause
SELECT x, y FROM t DISTRIBUTE BY x SORT BY y DESC;
```

## Cluster By

### Description

`CLUSTER BY` is a short-cut for both `DISTRIBUTE BY` and `SORT BY`.
The `CLUSTER BY` is used to first repartition the data based on the input expressions and sort the data with each partition.
Also, this clause only guarantees the data is sorted within each partition.

### Syntax

```sql
clusterBy: CLUSTER BY expression [ , ... ]
query: SELECT expression [ , ... ] FROM src clusterBy
```

### Examples

```sql
SELECT x, y FROM t CLUSTER BY x;
SELECT x, y FROM t CLUSTER BY abs(y);
```
