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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.table.runtime.types.CRow

trait DataStreamRel extends FlinkRelNode {

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @param queryConfig The configuration for the query to generate.
    * @return DataStream of type [[CRow]]
    */
  def translateToPlan(
    tableEnv: StreamTableEnvironment,
    queryConfig: StreamQueryConfig): DataStream[CRow]

  /**
    * Whether the [[DataStreamRel]] requires that update and delete changes are sent with retraction
    * messages.
    */
  def needsUpdatesAsRetraction: Boolean = false

  /**
    * Whether the [[DataStreamRel]] produces update and delete changes.
    */
  def producesUpdates: Boolean = false

  /**
    * Whether the [[DataStreamRel]] consumes retraction messages instead of forwarding them.
    * The node might or might not produce new retraction messages.
    */
  def consumesRetractions: Boolean = false

  /**
    * Whether the [[DataStreamRel]] produces retraction messages.
    * It might forward retraction messages nevertheless.
    */
  def producesRetractions: Boolean = false
}
