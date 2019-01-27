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

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, TimestampedCollector, TwoInputSelection, TwoInputStreamOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.collector.HeaderCollector
import org.apache.flink.table.util.Logging

class TemporalProcessTimeJoin(
    leftType: TypeInformation[BaseRow],
    rightType: TypeInformation[BaseRow],
    genJoinFuncName: String,
    genJoinFuncCode: String)
  extends AbstractStreamOperator[BaseRow]
  with TwoInputStreamOperator[BaseRow, BaseRow, BaseRow]
  with Compiler[FlatJoinFunction[BaseRow, BaseRow, BaseRow]]
  with Logging {

  protected var rightState: ValueState[BaseRow] = _
  protected var collector: TimestampedCollector[BaseRow] = _
  protected var headerCollector: HeaderCollector[BaseRow] = _

  protected var joinFunction: FlatJoinFunction[BaseRow, BaseRow, BaseRow] = _

  override def open(): Unit = {
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)

    joinFunction = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(joinFunction, getRuntimeContext)
    FunctionUtils.openFunction(joinFunction, new Configuration)

    val rightStateDescriptor = new ValueStateDescriptor[BaseRow]("right", rightType)
    rightState = getRuntimeContext.getState(rightStateDescriptor)

    collector = new TimestampedCollector[BaseRow](output)
    headerCollector = new HeaderCollector[BaseRow]
    headerCollector.out = collector
  }

  override def processElement1(element: StreamRecord[BaseRow]): TwoInputSelection = {

    if (rightState.value() == null) {
      return TwoInputSelection.ANY
    }

    headerCollector.setHeader(element.getValue.getHeader)

    val rightSideRow = rightState.value()
    joinFunction.join(element.getValue, rightSideRow, collector)

    TwoInputSelection.ANY
  }

  override def processElement2(element: StreamRecord[BaseRow]): TwoInputSelection = {

    if (BaseRowUtil.isAccumulateMsg(element.getValue)) {
      rightState.update(element.getValue)
    } else {
      rightState.clear()
    }
    TwoInputSelection.ANY
  }

  override def firstInputSelection(): TwoInputSelection = {
    TwoInputSelection.ANY
  }

  override def endInput1(): Unit = {}

  override def endInput2(): Unit = {}
}
