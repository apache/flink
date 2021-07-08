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

package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}

/**
  * providing join it case utility functions.
  */
object JoinITCaseHelper {

  def disableBroadcastHashJoin(tEnv: TableEnvironment): Unit = {
    tEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
  }

  def disableOtherJoinOpForJoin(tEnv: TableEnvironment, expected: JoinType): Unit = {
    val disabledOperators = expected match {
      case BroadcastHashJoin =>
        // set up the broadcast join threshold to Long.MaxValue
        // so that the threshold constraints are always met.
        tEnv.getConfig.getConfiguration.setLong(
          OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.MaxValue)
        "ShuffleHashJoin, NestedLoopJoin, SortMergeJoin"
      case HashJoin =>
        disableBroadcastHashJoin(tEnv)
        "NestedLoopJoin, SortMergeJoin"
      case SortMergeJoin => "HashJoin, NestedLoopJoin"
      case NestedLoopJoin => "HashJoin, SortMergeJoin"
    }
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, disabledOperators)
  }

}

object JoinType extends Enumeration {
  type JoinType = Value
  val BroadcastHashJoin, HashJoin, SortMergeJoin, NestedLoopJoin = Value
}
