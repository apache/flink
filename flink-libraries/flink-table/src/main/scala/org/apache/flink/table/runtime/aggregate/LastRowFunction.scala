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

package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.runtime.state.keyed.KeyedValueState
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.codegen.EqualiserCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.runtime.functions.{ExecutionContext, ProcessFunction}
import org.apache.flink.table.runtime.sort.RecordEqualiser
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Collector

/**
  * This function is used to get the last row for every key partition.
  *
  * @param rowTypeInfo        the typeInfo of the input row.
  * @param generateRetraction the flag whether to generate retractions in this operator.
  * @param rowtimeIndex       the index of rowtime field which is used to order data.
  * @param tableConfig        the table config.
  */
class LastRowFunction(
   rowTypeInfo: BaseRowTypeInfo,
   generateRetraction: Boolean,
   rowtimeIndex: Int,
   tableConfig: TableConfig)
  extends ProcessFunctionWithCleanupState[BaseRow, BaseRow](tableConfig)
  with LastRowFunctionBase
  with Logging {

  protected var pkRow: KeyedValueState[BaseRow, BaseRow] = _

  @transient
  private var equaliser: RecordEqualiser = _

  override def open(ctx: ExecutionContext): Unit = {
    super.open(ctx)
    initCleanupTimeState("LastRowFunctionCleanupTime")
    val rowStateDesc = new ValueStateDescriptor("rowState", rowTypeInfo)
    pkRow = ctx.getKeyedValueState(rowStateDesc)

    val generator = new EqualiserCodeGenerator(
      rowTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
    val generatedEqualiser = generator.generateRecordEqualiser("LastRowValueEqualiser")
    equaliser = generatedEqualiser.newInstance(ctx.getRuntimeContext.getUserCodeClassLoader)
  }

  override def processElement(
     input: BaseRow,
     ctx: ProcessFunction.Context,
     out: Collector[BaseRow]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    val currentKey = executionContext.currentKey()
    val preRow = pkRow.get(currentKey)
    processLastRow(
      currentKey,
      preRow,
      input,
      generateRetraction,
      rowtimeIndex,
      stateCleaningEnabled,
      pkRow,
      equaliser,
      out)
  }

  override def close(): Unit = super.close()

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction.OnTimerContext,
      out: Collector[BaseRow]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(pkRow)
    }
  }
}
