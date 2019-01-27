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

package org.apache.flink.table.runtime.join

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.FlinkJoinRelType

/**
  * The function to execute processing time bounded stream inner-join.
  */
final class ProcTimeBoundedStreamJoin(
    joinType: FlinkJoinRelType,
    leftLowerBound: Long,
    leftUpperBound: Long,
    leftType: TypeInformation[BaseRow],
    rightType: TypeInformation[BaseRow],
    genJoinFuncName: String,
    genJoinFuncCode: String)
  extends TimeBoundedStreamJoin(joinType,
    leftLowerBound,
    leftUpperBound,
    allowLateness = 0L,
    leftType,
    rightType,
    genJoinFuncName,
    genJoinFuncCode) {

  override def updateOperatorTime(ctx: CoProcessFunction[BaseRow, BaseRow,
      BaseRow]#Context): Unit = {
    leftOperatorTime = ctx.timerService().currentProcessingTime()
    rightOperatorTime = leftOperatorTime
  }

  override def getTimeForLeftStream(
      ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
      row: BaseRow): Long = {
    leftOperatorTime
  }

  override def getTimeForRightStream(ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
      row: BaseRow): Long = {
    rightOperatorTime
  }

  override def registerTimer(
      ctx: CoProcessFunction[BaseRow, BaseRow, BaseRow]#Context,
      cleanupTime: Long): Unit = {
    ctx.timerService().registerProcessingTimeTimer(cleanupTime)
  }
}
