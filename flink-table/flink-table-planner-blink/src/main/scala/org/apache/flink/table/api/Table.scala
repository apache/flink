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

import org.apache.calcite.rel.RelNode

/**
  * A Table is the core component of the Table API.
  * Similar to how the batch and streaming APIs have DataSet and DataStream,
  * the Table API is built around [[Table]].
  *
  * NOTE: This class is only a placeholder to support end-to-end tests for Blink planner.
  * Will be removed when [[Table]] is moved to "flink-table-api-java" module.
  *
  * @param tableEnv The [[TableEnvironment]] to which the table is bound.
  * @param relNode  The Calcite RelNode representation
  */
class Table(val tableEnv: TableEnvironment, relNode: RelNode) {

  /**
    * Returns the Calcite RelNode represent this Table.
    */
  def getRelNode: RelNode = relNode

}
