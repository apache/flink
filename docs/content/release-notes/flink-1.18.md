---
title: "Release Notes - Flink 1.18"
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

# Release notes - Flink 1.18

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.17 and Flink 1.18. Please read these notes carefully if you are
planning to upgrade your Flink version to 1.18.


### Build System

#### Support Java 17 (LTS)

##### [FLINK-15736](https://issues.apache.org/jira/browse/FLINK-15736)
Apache Flink was made ready to compile and run with Java 17 (LTS). This feature is still in beta mode. 
Issues should be reported in Flink's bug tracker.


### Table API & SQL

#### Unified the max display column width for SQL Client and Table APi in both Streaming and Batch execution Mode

##### [FLINK-30025](https://issues.apache.org/jira/browse/FLINK-30025)
Introduction of the new ConfigOption `DISPLAY_MAX_COLUMN_WIDTH` (`table.display.max-column-width`)  
in the TableConfigOptions class is now in place. 
This option is utilized when displaying table results through the Table API and SQL Client. 
As SQL Client relies on the Table API underneath, and both SQL Client and the Table API serve distinct 
and isolated scenarios, it is a rational choice to maintain a centralized configuration. 
This approach also simplifies matters for users, as they only need to manage one ConfigOption for display control.

During the migration phase, while `sql-client.display.max-column-width` is deprecated, 
any changes made to `sql-client.display.max-column-width` will be automatically transferred to `table.display.max-column-width`. 
Caution is advised when using the CLI, as it is not recommended to switch back and forth between these two options.

#### Introduce Flink JDBC Driver For SQL Gateway
##### [FLINK-31496](https://issues.apache.org/jira/browse/FLINK-31496)
Apache Flink now supports JDBC driver to access SQL Gateway, you can use the driver in any cases that
support standard JDBC extension to connect to Flink cluster.

#### Extend watermark-related features for SQL
##### [FLINK-31535](https://issues.apache.org/jira/browse/FLINK-31535)
Flink now enables user config watermark emit strategy/watermark alignment/watermark idle-timeout
in Flink SQL job with dynamic table options and 'OPTIONS' hint.

#### Support configuring CatalogStore in Table API
##### [FLINK-32431](https://issues.apache.org/jira/browse/FLINK-32431)
Support lazy initialization of catalog and persistence of catalog configuration.

#### Deprecate ManagedTable related APIs
##### [FLINK-32656](https://issues.apache.org/jira/browse/FLINK-32656)
ManagedTable related APIs are deprecated and will be removed in a future major release.

### Connectors & Libraries

#### SplitReader implements AutoCloseable instead of providing its own close method
##### [FLINK-31015](https://issues.apache.org/jira/browse/FLINK-31015)
SplitReader interface now extends `AutoCloseable` instead of providing its own method signature.

#### JSON format supports projection push down
##### [FLINK-32610](https://issues.apache.org/jira/browse/FLINK-32610)
The JSON format introduced JsonParser as a new default way to deserialize JSON data. 
JsonParser is a Jackson JSON streaming API to read JSON data which is much faster 
and consumes less memory compared to the previous JsonNode approach. 
This should be a compatible change, if you encounter any issues after upgrading, 
you can fallback to the previous JsonNode approach by setting `json.decode.json-parser.enabled` to `false`. 



### Runtime & Coordination

#### Unifying the Implementation of SlotManager
##### [FLINK-31439](https://issues.apache.org/jira/browse/FLINK-31439)
Fine-grained resource management are now enabled by default. You can use it by specifying the resource requirement. 
More details can be found at https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/finegrained_resource/#usage.

#### Watermark aggregation performance is poor when watermark alignment is enabled and parallelism is high
##### [FLINK-32420](https://issues.apache.org/jira/browse/FLINK-32420)
This performance improvement would be good to mention in the release blog post. 

As proven by the micro benchmarks (screenshots attached in the ticket), with 5000 subtasks, 
the time to calculate the watermark alignment on the JobManager by a factor of 76x (7664%). 
Previously such large jobs were actually at large risk of overloading JobManager, now that's far less likely to happen.

#### Replace Akka by Pekko
##### [FLINK-32468](https://issues.apache.org/jira/browse/32468)
Flink's RPC framework is now based on Apache Pekko instead of Akka. Any Akka dependencies were removed.

#### Introduce Runtime Filter for Flink Batch Jobs
##### [FLINK-32486](https://issues.apache.org/jira/browse/FLINK-32486)
We introduced a runtime filter for batch jobs in 1.18, which is designed to improve join performance. 
It will dynamically generate filter conditions for certain Join queries at runtime to reduce the amount of scanned or shuffled data, 
avoid unnecessary I/O and network transmission, and speed up the query. 
Its working principle is building a filter(e.g. bloom filter) based on the data on the small table side(build side) first, 
then passing this filter to the large table side(probe side) to filter the irrelevant data on it, 
this can reduce the data reaching the join and improve performance.

#### Make watermark alignment ready for production use
##### [FLINK-32548](https://issues.apache.org/jira/browse/FLINK-32548)
The watermark alignment is ready for production since Flink 1.18, 
which completed a series of bug fixes and improvements related to watermark alignment. 
Please refer to [FLINK-32420](https://issues.apache.org/jira/browse/FLINK-32420) for more information.

#### Redundant TaskManagers should always be fulfilled in FineGrainedSlotManager
##### [FLINK-32880](https://issues.apache.org/jira/browse/FLINK-32880)
Fix the issue that redundant TaskManagers will not be fulfilled in FineGrainedSlotManager periodically.

#### RestClient can deadlock if request made after Netty event executor terminated
##### [FLINK-32583](https://issues.apache.org/jira/browse/FLINK-32583)
Fix a bug in the RestClient where making a request after the client was closed returns a future that never completes.

#### Deprecate Queryable State
##### [FLINK-32559](https://issues.apache.org/jira/browse/FLINK-32559)
The Queryable State feature is formally deprecated. It will be removed in future major version bumps.


### SDK

#### Properly deprecate DataSet API
##### [FLINK-32558](https://issues.apache.org/jira/browse/FLINK-32558)
DataSet API is formally deprecated, and will be removed in the next major release.


### Dependency upgrades

#### Upgrade Calcite version to 1.32.0
##### [FLINK-29319](https://issues.apache.org/jira/browse/FLINK-29319) and related tickets [FLINK-27998](https://issues.apache.org/jira/browse/FLINK-27998), [FLINK-28744](https://issues.apache.org/jira/browse/FLINK-28744)

Due to CALCITE-4861 (Optimization of chained CAST calls can lead to unexpected behavior), 
also Flink's casting behavior has slightly changed. Some corner cases might behave differently now: For example, 
casting from `FLOAT`/`DOUBLE` 9234567891.12 to `INT`/`BIGINT` has now Java behavior for overflows.

