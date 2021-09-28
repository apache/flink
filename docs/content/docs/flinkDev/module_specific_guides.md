---
title: "Module specific guides"
weight: 22
type: docs
aliases:
- /flinkDev/module_specific_guides.html
- /internals/module_specific_guides.html
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

# Module specific guides

This page includes a list of guides, tips and tricks for contributing to specific Flink modules. 

## Flink Table/SQL

### Debug the planner

When working on planner rules, It's useful to investigate which rules are applied, in which order and what transformation every rule generates. 
In order to find out these details when running a planner test, you can enable the Calcite planner logging in debug mode adding the following lines to the log configuration in `flink-table/flink-table-planner/src/test/resources/log4j2-test.properties`:

```
loggers = testlogger
logger.testlogger.name = org.apache.calcite.plan
logger.testlogger.level = DEBUG
logger.testlogger.appenderRefs = TestLogger
```

This is a sample output:

```
[...]
3855 [main] DEBUG org.apache.calcite.plan.RelOptPlanner [] - call#1: Apply rule [FlinkJoinPushExpressionsRule] to [rel#40:LogicalJoin.NONE.any.None: 0.[NONE].[NONE](left=HepRelVertex#34,right=HepRelVertex#39,condition=AND(=($0, $5), >=($3, -($8, 3600000:INTERVAL HOUR)), <=($3, +($8, 3600000:INTERVAL HOUR))),joinType=inner)]
3855 [main] DEBUG org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary [] - Rule Attempts Info for HepPlanner
3856 [main] DEBUG org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary [] - 
Rules                                                                   Attempts           Time (us)
FlinkJoinPushExpressionsRule                                                   1                 143
* Total                                                                        1                 143
[...]
```
