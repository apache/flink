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
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.CRowWrappingCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

class TemporalJoin(
    leftType: TypeInformation[Row],
    rightType: TypeInformation[Row],
    genJoinFuncName: String,
    genJoinFuncCode: String,
    queryConfig: StreamQueryConfig)
  extends CoProcessFunction[CRow, CRow, CRow]
  with Compiler[FlatJoinFunction[Row, Row, Row]]
  with Logging {

  validateEqualsHashCode("join", leftType)
  validateEqualsHashCode("join", rightType)

  protected var rightState: ValueState[Row] = _
  protected var cRowWrapper: CRowWrappingCollector = _

  protected var joinFunction: FlatJoinFunction[Row, Row, Row] = _

  override def open(parameters: Configuration): Unit = {
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genJoinFuncName,
      genJoinFuncCode)

    joinFunction = clazz.newInstance()

    val rightStateDescriptor = new ValueStateDescriptor[Row]("right", rightType)
    rightState = getRuntimeContext.getState(rightStateDescriptor)

    cRowWrapper = new CRowWrappingCollector()
  }

  override def processElement1(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    if (rightState.value() == null) {
      return
    }

    cRowWrapper.out = out
    cRowWrapper.setChange(value.change)

    val rightSideRow = rightState.value()
    joinFunction.join(value.row, rightSideRow, cRowWrapper)
  }

  override def processElement2(
      value: CRow,
      ctx: CoProcessFunction[CRow, CRow, CRow]#Context,
      out: Collector[CRow]): Unit = {

    if (value.change) {
      rightState.update(value.row)
    } else {
      rightState.clear()
    }
  }
}
