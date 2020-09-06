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
import org.apache.flink.runtime.state.VoidNamespace
import org.apache.flink.streaming.api.operators.{InternalTimer, TimestampedCollector}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

class TemporalProcessTimeJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    minRetentionTime: Long,
    maxRetentionTime: Long)
  extends BaseTwoInputStreamOperatorWithStateRetention(minRetentionTime, maxRetentionTime)
  with Compiler[FlatJoinFunction[Row, Row, Row]]
  with Logging {

  validateEqualsHashCode("join", leftType)
  validateEqualsHashCode("join", rightType)

  protected var rightState: ValueState[Row] = _
  protected var cRowWrapper: CRowWrappingCollector = _
  protected var collector: TimestampedCollector[CRow] = _

  protected var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  override def open(): Unit = {
    LOG.debug(s"Compiling FlatJoinFunction: $genJoinFuncName \n\n Code:\n$genJoinFuncCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)

    LOG.debug("Instantiating FlatJoinFunction.")
    joinFunction = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(joinFunction, getRuntimeContext)
    FunctionUtils.openFunction(joinFunction, new Configuration())

    val rightStateDescriptor = new ValueStateDescriptor[Row]("right", rightType)
    rightState = getRuntimeContext.getState(rightStateDescriptor)

    collector = new TimestampedCollector[CRow](output)
    cRowWrapper = new CRowWrappingCollector()
    cRowWrapper.out = collector

    super.open()
  }

  override def processElement1(element: StreamRecord[CRow]): Unit = {

    val rightSideRow = rightState.value()
    if (rightSideRow == null) {
      return
    }

    cRowWrapper.setChange(element.getValue.change)

    joinFunction.join(element.getValue.row, rightSideRow, cRowWrapper)

    registerProcessingCleanUpTimer()
  }

  override def processElement2(element: StreamRecord[CRow]): Unit = {

    if (element.getValue.change) {
      rightState.update(element.getValue.row)
      registerProcessingCleanUpTimer()
    } else {
      rightState.clear()
      cleanUpLastTimer()
    }
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(joinFunction)
  }

  /**
    * The method to be called when a cleanup timer fires.
    *
    * @param time The timestamp of the fired timer.
    */
  override def cleanUpState(time: Long): Unit = {
    rightState.clear()
  }

  /**
    * Invoked when an event-time timer fires.
    */
  override def onEventTime(timer: InternalTimer[Any, VoidNamespace]): Unit = ???
}
