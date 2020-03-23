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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore
import org.apache.flink.table.runtime.generated.AggsHandleFunction
import org.apache.flink.table.types.utils.TypeConversions

import org.junit.{Assert, Test}

import java.lang

class AggsHandlerCodeGeneratorTest extends AggTestBase(isBatchMode = false) {

  @Test
  def testAvg(): Unit = {
    val handler = getHandler(needRetract = false, needMerge = false)
    handler.resetAccumulators()
    handler.accumulate(GenericRow.of("f0", jl(5L), jd(5.3D), jl(2L)))
    handler.accumulate(GenericRow.of("f0", jl(6L), jd(6.5D), jl(3L)))
    handler.accumulate(GenericRow.of("f0", jl(7L), jd(7.1D), jl(4L)))
    val ret = handler.getValue
    Assert.assertEquals(6L, ret.getLong(0), 0)
    Assert.assertEquals(6.3, ret.getDouble(1), 0)
    Assert.assertEquals(3L, ret.getLong(2), 0)
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
    Assert.assertEquals(4L, ret.getLong(0), 0)
    Assert.assertEquals(6.75, ret.getDouble(1), 0)
    Assert.assertEquals(2L, ret.getLong(2), 0)
  }

  @Test
  def testAvgWithMerge(): Unit = {
    val handler = getHandler(needRetract = false, needMerge = true)
    handler.resetAccumulators()
    handler.merge(GenericRow.of("f0", jl(50L), jl(2L), jd(5D), jl(2L), jt(50L, 2L)))
    handler.merge(GenericRow.of("f0", jl(40L), jl(2L), jd(4D), jl(2L), jt(40L, 2L)))
    handler.merge(GenericRow.of("f0", jl(43L), jl(1L), jd(4D), jl(1L), jt(43L, 1L)))
    val ret = handler.getValue
    Assert.assertEquals(26L, ret.getLong(0), 0)
    Assert.assertEquals(2.6, ret.getDouble(1), 0)
    Assert.assertEquals(26L, ret.getLong(2), 0)
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
    val generator = new AggsHandlerCodeGenerator(ctx, relBuilder, inputTypes, true)
    if (needRetract) {
      generator.needRetract()
    }
    if (needMerge) {
      generator.needMerge(1, mergedAccOnHeap = true,
        Array(DataTypes.BIGINT, DataTypes.BIGINT, DataTypes.DOUBLE, DataTypes.BIGINT,
          TypeConversions.fromLegacyInfoToDataType(imperativeAggFunc.getAccumulatorType)))
    }
    val handler = generator
      .needAccumulate()
      .generateAggsHandler("Test", aggInfoList).newInstance(classLoader)
    handler.open(new PerKeyStateDataViewStore(context.getRuntimeContext))
    handler
  }
}
