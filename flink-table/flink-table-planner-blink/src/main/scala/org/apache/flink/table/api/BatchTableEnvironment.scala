/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * The abstract base class for batch TableEnvironments.
  *
  * A TableEnvironment can be used to:
  * - convert a bounded [[DataStream]] to a [[Table]]
  * - register a bounded [[DataStream]] in the [[TableEnvironment]]'s catalog
  * - register a [[Table]] in the [[TableEnvironment]]'s catalog
  * - scan a registered table to obtain a [[Table]]
  * - specify a SQL query on registered tables to obtain a [[Table]]
  * - explain the AST and execution plan of a [[Table]]
  *
  * @param execEnv The [[StreamExecutionEnvironment]] which is wrapped in this
  *                [[BatchTableEnvironment]].
  * @param config The [[TableConfig]] of this [[StreamTableEnvironment]].
  */
abstract class BatchTableEnvironment(
    private[flink] val execEnv: StreamExecutionEnvironment,
    config: TableConfig)
  extends TableEnvironment(config) {

  // TODO: will add or implement some methods here

}
