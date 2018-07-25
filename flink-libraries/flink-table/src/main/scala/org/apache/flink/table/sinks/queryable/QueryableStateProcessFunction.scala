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

package org.apache.flink.table.sinks.queryable

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.KeyedProcessFunctionWithCleanupState
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

class QueryableStateProcessFunction(
  private val namePrefix: String,
  private val queryConfig: StreamQueryConfig,
  private val keyNames: Array[String],
  private val fieldNames: Array[String],
  private val fieldTypes: Array[TypeInformation[_]])
  extends KeyedProcessFunctionWithCleanupState[Row, JTuple2[JBool, Row], Void](queryConfig) {

  @transient private var states: Array[ValueState[AnyRef]] = _
  @transient private var nonKeyIndices: Array[Int] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    nonKeyIndices = fieldNames.indices
      .filter(idx => !keyNames.contains(fieldNames(idx)))
      .toArray

    val statesBuilder = Array.newBuilder[ValueState[AnyRef]]

    for (i <- nonKeyIndices) {
      val stateDesc = new ValueStateDescriptor(fieldNames(i), fieldTypes(i))
      stateDesc.initializeSerializerUnlessSet(getRuntimeContext.getExecutionConfig)
      stateDesc.setQueryable(s"$namePrefix-${fieldNames(i)}")
      statesBuilder += getRuntimeContext.getState(stateDesc).asInstanceOf[ValueState[AnyRef]]
    }

    states = statesBuilder.result()

    initCleanupTimeState("QueryableStateCleanupTime")
  }

  override def processElement(
    value: JTuple2[JBool, Row],
    ctx: KeyedProcessFunction[Row, JTuple2[JBool, Row], Void]#Context,
    out: Collector[Void]): Unit = {
    if (value.f0) {
      var i = 0
      while (i < nonKeyIndices.length) {
        states(i).update(value.f1.getField(nonKeyIndices(i)))
        i += 1
      }

      val currentTime: Long = ctx.timerService().currentProcessingTime()
      registerProcessingCleanupTimer(ctx, currentTime)
    } else {
      cleanupState(states: _*)
    }
  }

  override def onTimer(
    timestamp: Long,
    ctx: KeyedProcessFunction[Row, JTuple2[JBool, Row], Void]#OnTimerContext,
    out: Collector[Void]): Unit = {
    if (needToCleanupState(timestamp)) {
      cleanupState(states: _*)
    }
  }
}
