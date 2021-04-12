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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink
import org.apache.flink.table.sinks.UpsertStreamTableSink

import scala.collection.JavaConversions._

object UpdatingPlanChecker {

  def getUniqueKeyForUpsertSink(
      sinkNode: LegacySink,
      sink: UpsertStreamTableSink[_]): Option[Array[String]] = {
    // extract unique key fields
    // Now we pick shortest one to sink
    // TODO UpsertStreamTableSink setKeyFields interface should be Array[Array[String]]
    val sinkFieldNames = sink.getTableSchema.getFieldNames
    /** Extracts the unique keys of the table produced by the plan. */
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(sinkNode.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(sinkNode.getInput)
    if (uniqueKeys != null && uniqueKeys.size() > 0) {
      uniqueKeys
          .filter(_.nonEmpty)
          .map(_.toArray.map(sinkFieldNames))
          .toSeq
          .sortBy(_.length)
          .headOption
    } else {
      None
    }
  }
}
