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

package org.apache.flink.table.codegen.agg

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.table.`type`.{InternalType, InternalTypes, RowType}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.functions.AvgAggFunction.{DoubleAvgAggFunction, IntegralAvgAggFunction}
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateInfoList}
import org.apache.flink.table.runtime.context.ExecutionContext

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.{FrameworkConfig, RelBuilder}
import org.powermock.api.mockito.PowerMockito.{mock, when}

/**
  * Agg test base to mock agg information and etc.
  */
abstract class AggTestBase {

  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  val env = new LocalStreamEnvironment
  val conf = new TableConfig
  val tEnv = new StreamTableEnvironment(env, conf)
  val frameworkConfig: FrameworkConfig = tEnv.getFrameworkConfig
  val inputNames = Array("f0", "f1", "f2", "f3", "f4")
  val inputTypes: Array[InternalType] = Array(
    InternalTypes.STRING, InternalTypes.LONG, InternalTypes.DOUBLE, InternalTypes.LONG,
    InternalTypes.STRING)
  val inputType = new RowType(inputTypes, inputNames)

  val relBuilder: RelBuilder = FlinkRelBuilder.create(frameworkConfig).values(
    typeFactory.buildLogicalRowType(inputNames, inputTypes))
  val aggInfo1: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo, "agg").thenReturn(call)
    when(call, "getName").thenReturn("avg1")
    when(aggInfo, "function").thenReturn(new IntegralAvgAggFunction)
    when(aggInfo, "externalAccTypes").thenReturn(Array(Types.LONG, Types.LONG))
    when(aggInfo, "argIndexes").thenReturn(Array(1))
    when(aggInfo, "aggIndex").thenReturn(0)
    aggInfo
  }

  val aggInfo2: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo, "agg").thenReturn(call)
    when(call, "getName").thenReturn("avg2")
    when(aggInfo, "function").thenReturn(new DoubleAvgAggFunction)
    when(aggInfo, "externalAccTypes").thenReturn(Array(Types.DOUBLE, Types.LONG))
    when(aggInfo, "argIndexes").thenReturn(Array(2))
    when(aggInfo, "aggIndex").thenReturn(1)
    aggInfo
  }

  val imperativeAggFunc = new TestLongAvgFunc
  val aggInfo3: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo, "agg").thenReturn(call)
    when(call, "getName").thenReturn("avg3")
    when(aggInfo, "function").thenReturn(imperativeAggFunc)
    when(aggInfo, "externalAccTypes").thenReturn(Array(imperativeAggFunc.getAccumulatorType))
    when(aggInfo, "externalResultType").thenReturn(Types.DOUBLE)
    when(aggInfo, "viewSpecs").thenReturn(Array[DataViewSpec]())
    when(aggInfo, "argIndexes").thenReturn(Array(3))
    when(aggInfo, "aggIndex").thenReturn(2)
    aggInfo
  }

  val aggInfoList = AggregateInfoList(
    Array(aggInfo1, aggInfo2, aggInfo3), None, count1AggInserted = false, Array())
  val ctx = new CodeGeneratorContext(conf)
  val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  val context: ExecutionContext = mock(classOf[ExecutionContext])
  when(context, "getRuntimeContext").thenReturn(mock(classOf[RuntimeContext]))
}
