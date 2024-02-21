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
package org.apache.flink.table.planner.codegen.agg

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction.{DoubleAvgAggFunction, LongAvgAggFunction}
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, AggregateInfoList}
import org.apache.flink.table.runtime.context.ExecutionContext
import org.apache.flink.table.runtime.dataview.DataViewSpec
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.RelBuilder
import org.mockito.Mockito.{mock, when}

/** Agg test base to mock agg information and etc. */
abstract class AggTestBase(isBatchMode: Boolean) {

  val typeFactory: FlinkTypeFactory =
    new FlinkTypeFactory(Thread.currentThread().getContextClassLoader, FlinkTypeSystem.INSTANCE)
  val env = new ScalaStreamExecEnv(new LocalStreamEnvironment)
  private val tEnv = if (isBatchMode) {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    // use impl class instead of interface class to avoid
    // "Static methods in interface require -target:jvm-1.8"
    TableEnvironmentImpl.create(settings)
  } else {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    StreamTableEnvironment.create(env, settings)
  }
  private val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  val inputNames = Array("f0", "f1", "f2", "f3", "f4")
  val inputTypes: Array[LogicalType] = Array(
    VarCharType.STRING_TYPE,
    new BigIntType(),
    new DoubleType(),
    new BigIntType(),
    VarCharType.STRING_TYPE)
  val inputType: RowType = RowType.of(inputTypes, inputNames)

  val relBuilder: RelBuilder =
    planner.createRelBuilder.values(typeFactory.buildRelNodeRowType(inputNames, inputTypes))
  val aggInfo1: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo.agg).thenReturn(call)
    when(call.getName).thenReturn("avg1")
    when(call.hasFilter).thenReturn(false)
    when(aggInfo.function).thenReturn(new LongAvgAggFunction)
    when(aggInfo.externalArgTypes).thenReturn(Array(DataTypes.BIGINT))
    when(aggInfo.externalAccTypes).thenReturn(Array(DataTypes.BIGINT, DataTypes.BIGINT))
    when(aggInfo.argIndexes).thenReturn(Array(1))
    when(aggInfo.aggIndex).thenReturn(0)
    when(aggInfo.externalResultType).thenReturn(DataTypes.BIGINT)
    aggInfo
  }

  val aggInfo2: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo.agg).thenReturn(call)
    when(call.getName).thenReturn("avg2")
    when(call.hasFilter).thenReturn(false)
    when(aggInfo.function).thenReturn(new DoubleAvgAggFunction)
    when(aggInfo.externalArgTypes).thenReturn(Array(DataTypes.DOUBLE()))
    when(aggInfo.externalAccTypes).thenReturn(Array(DataTypes.DOUBLE, DataTypes.BIGINT))
    when(aggInfo.argIndexes).thenReturn(Array(2))
    when(aggInfo.aggIndex).thenReturn(1)
    when(aggInfo.externalResultType).thenReturn(DataTypes.DOUBLE)
    aggInfo
  }

  val imperativeAggFunc = new TestLongAvgFunc
  val aggInfo3: AggregateInfo = {
    val aggInfo = mock(classOf[AggregateInfo])
    val call = mock(classOf[AggregateCall])
    when(aggInfo.agg).thenReturn(call)
    when(call.getName).thenReturn("avg3")
    when(call.hasFilter).thenReturn(false)
    when(aggInfo.function).thenReturn(imperativeAggFunc)
    when(aggInfo.externalArgTypes).thenReturn(Array(DataTypes.BIGINT()))
    when(aggInfo.externalAccTypes).thenReturn(
      Array(fromLegacyInfoToDataType(imperativeAggFunc.getAccumulatorType)))
    when(aggInfo.externalResultType).thenReturn(DataTypes.BIGINT)
    when(aggInfo.viewSpecs).thenReturn(Array[DataViewSpec]())
    when(aggInfo.argIndexes).thenReturn(Array(3))
    when(aggInfo.aggIndex).thenReturn(2)
    aggInfo
  }

  val aggInfoList =
    AggregateInfoList(Array(aggInfo1, aggInfo2, aggInfo3), None, countStarInserted = false, Array())
  val ctx = new CodeGeneratorContext(tEnv.getConfig, Thread.currentThread().getContextClassLoader)
  val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  val context: ExecutionContext = mock(classOf[ExecutionContext])
  when(context.getRuntimeContext).thenReturn(mock(classOf[RuntimeContext]))
}
