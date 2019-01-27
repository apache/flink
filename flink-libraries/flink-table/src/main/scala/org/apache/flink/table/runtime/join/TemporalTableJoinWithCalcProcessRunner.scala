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

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Collector

class TemporalTableJoinWithCalcProcessRunner(
    fetcherName: String,
    fetcherCode: String,
    calcFunctionName: String,
    var calcFunctionCode: String,
    collectorName: String,
    collectorCode: String,
    leftOuterJoin: Boolean,
    inputFieldTypes: Array[InternalType],
  @transient returnType: BaseRowTypeInfo)
  extends TemporalTableJoinProcessRunner(
    fetcherName,
    fetcherCode,
    collectorName,
    collectorCode,
    leftOuterJoin,
    inputFieldTypes,
    returnType) {

  private var calc: FlatMapFunction[BaseRow, BaseRow] = _
  private var calcCollector: Collector[BaseRow] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    LOG.debug(s"Compiling CalcFunction: $calcFunctionName \n\n Code:\n$calcFunctionCode")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      calcFunctionName,
      calcFunctionCode)
    calcFunctionCode = null

    LOG.debug("Instantiating CalcFunction.")
    calc = clazz.newInstance().asInstanceOf[FlatMapFunction[BaseRow, BaseRow]]
    calcCollector = new CalcCollector(calc, collector)
  }

  override def getFetcherCollector: Collector[BaseRow] = calcCollector

  class CalcCollector(
      calcFlatMap: FlatMapFunction[BaseRow, BaseRow],
      delegate: Collector[BaseRow])
    extends Collector[BaseRow] {

    override def collect(t: BaseRow): Unit = calcFlatMap.flatMap(t, delegate)

    override def close(): Unit = delegate.close()
  }
}
