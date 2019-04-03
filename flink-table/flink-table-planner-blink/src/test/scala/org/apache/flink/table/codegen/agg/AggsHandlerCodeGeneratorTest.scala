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
import org.apache.flink.table.`type`.InternalTypes
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.codegen.CodeGeneratorContext
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.dataview.DataViewSpec
import org.apache.flink.table.functions.AvgAggFunction.{DoubleAvgAggFunction, IntegralAvgAggFunction}
import org.apache.flink.table.generated.AggsHandleFunction
import org.apache.flink.table.plan.util.{AggregateInfo, AggregateInfoList}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.tools.FrameworkConfig
import org.junit.{Assert, Test}
import org.powermock.api.mockito.PowerMockito.{mock, when}
import java.lang

import org.apache.flink.table.runtime.context.ExecutionContext

class AggsHandlerCodeGeneratorTest {

  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  private val env = new LocalStreamEnvironment
  private val conf = new TableConfig
  private val tEnv = new StreamTableEnvironment(env, conf)
  private val frameworkConfig: FrameworkConfig = tEnv.getFrameworkConfig
  private val inputNames = Array("f0", "f1", "f2", "f3")
  private val inputTypes = Array(
    InternalTypes.STRING,
    InternalTypes.LONG, InternalTypes.DOUBLE, InternalTypes.LONG)
  private val relBuilder = FlinkRelBuilder.create(frameworkConfig).values(
    typeFactory.buildLogicalRowType(inputNames, inputTypes))
  private val aggInfo1 = {
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

  private val aggInfo2 = {
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

  private val imperativeAggFunc = new TestLongAvgFunc
  private val aggInfo3 = {
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

  private val aggInfoList = AggregateInfoList(
    Array(aggInfo1, aggInfo2, aggInfo3), None, count1AggInserted = false, Array())
  private val ctx = new CodeGeneratorContext(conf)
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val context = mock(classOf[ExecutionContext])
  when(context, "getRuntimeContext").thenReturn(mock(classOf[RuntimeContext]))

  @Test
  def testAvg(): Unit = {
    val handler = getHandler(needRetract = false, needMerge = false)
    handler.resetAccumulators()
    handler.accumulate(GenericRow.of("f0", jl(5L), jd(5.3D), jl(2L)))
    handler.accumulate(GenericRow.of("f0", jl(6L), jd(6.5D), jl(3L)))
    handler.accumulate(GenericRow.of("f0", jl(7L), jd(7.1D), jl(4L)))
    val ret = handler.getValue
    Assert.assertEquals(6.0, ret.getDouble(0), 0)
    Assert.assertEquals(6.3, ret.getDouble(1), 0)
    Assert.assertEquals(3.0, ret.getDouble(2), 0)
  }

  @Test
  def testAvgWithRetract(): Unit = {
    val handler = getHandler(needRetract = true, needMerge = false)
    handler.resetAccumulators()
    handler.accumulate(GenericRow.of("f0", jl(5L), jd(5.3D), jl(2L)))
    handler.accumulate(GenericRow.of("f0", jl(6L), jd(6.3D), jl(3L)))
    handler.accumulate(GenericRow.of("f0", jl(7L), jd(7.4D), jl(4L)))
    handler.retract(GenericRow.of("f0", jl(9L), jd(5.5D), jl(5L)))
    val ret = handler.getValue
    Assert.assertEquals(4.0, ret.getDouble(0), 0)
    Assert.assertEquals(6.75, ret.getDouble(1), 0)
    Assert.assertEquals(2.0, ret.getDouble(2), 0)
  }

  @Test
  def testAvgWithMerge(): Unit = {
    val handler = getHandler(needRetract = false, needMerge = true)
    handler.resetAccumulators()
    handler.merge(GenericRow.of("f0", jl(50L), jl(2L), jd(5D), jl(2L), jt(50L, 2L)))
    handler.merge(GenericRow.of("f0", jl(40L), jl(2L), jd(4D), jl(2L), jt(40L, 2L)))
    handler.merge(GenericRow.of("f0", jl(43L), jl(1L), jd(4D), jl(1L), jt(43L, 1L)))
    val ret = handler.getValue
    // TODO return 26.6 instead of 26.0 after divide return double instead of long
    Assert.assertEquals(26.0, ret.getDouble(0), 0)
    Assert.assertEquals(2.6, ret.getDouble(1), 0)
    Assert.assertEquals(26.0, ret.getDouble(2), 0)
  }

  private def jl(l: Long): lang.Long = {
    new lang.Long(l)
  }

  private def jd(l: Double): lang.Double = {
    new lang.Double(l)
  }

  private def jt(l1: Long, l2: Long): GenericRow = {
    GenericRow.of(jl(l1), jl(l2))
  }

  private def getHandler(needRetract: Boolean, needMerge: Boolean): AggsHandleFunction = {
    val generator = new AggsHandlerCodeGenerator(ctx, relBuilder, inputTypes, needRetract, true)
    if (needMerge) {
      generator.withMerging(1, mergedAccOnHeap = true, Array(Types.LONG, Types.LONG,
        Types.DOUBLE, Types.LONG, imperativeAggFunc.getAccumulatorType))
    }
    val handler = generator.generateAggsHandler("Test", aggInfoList).newInstance(classLoader)
    handler.open(context)
    handler
  }
}
