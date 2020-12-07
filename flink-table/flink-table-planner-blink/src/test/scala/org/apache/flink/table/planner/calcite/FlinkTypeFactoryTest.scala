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

package org.apache.flink.table.planner.calcite

import java.time.DayOfWeek

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot
import org.apache.flink.table.types.logical._
import org.junit.{Assert, Test}

class FlinkTypeFactoryTest {

  @Test
  def testInternalToRelType(): Unit = {
    val typeFactory = FlinkTypeFactory.INSTANCE

    def test(t: LogicalType): Unit = {
      Assert.assertEquals(
        t.copy(true),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(true)))
      )

      if (!hasRoot(t, LogicalTypeRoot.NULL)) {
        Assert.assertEquals(
          t.copy(false),
          FlinkTypeFactory.toLogicalType(
            typeFactory.createFieldTypeFromLogicalType(t.copy(false)))
        )
      }

      // twice for cache.
      Assert.assertEquals(
        t.copy(true),
        FlinkTypeFactory.toLogicalType(
          typeFactory.createFieldTypeFromLogicalType(t.copy(true)))
      )

      if (!hasRoot(t, LogicalTypeRoot.NULL)) {
        Assert.assertEquals(
          t.copy(false),
          FlinkTypeFactory.toLogicalType(
            typeFactory.createFieldTypeFromLogicalType(t.copy(false)))
        )
      }
    }

    test(new NullType())
    test(new BooleanType())
    test(new TinyIntType())
    test(new VarCharType(VarCharType.MAX_LENGTH))
    test(new DoubleType())
    test(new FloatType())
    test(new IntType())
    test(new BigIntType())
    test(new SmallIntType())
    test(new VarBinaryType(VarBinaryType.MAX_LENGTH))
    test(new DateType())
    test(new TimeType())
    test(new TimestampType(3))
    test(new LocalZonedTimestampType(3))

    test(new ArrayType(new DoubleType()))
    test(new MapType(new DoubleType(), new VarCharType(VarCharType.MAX_LENGTH)))
    test(RowType.of(new DoubleType(), new VarCharType(VarCharType.MAX_LENGTH)))
    test(new RawType[DayOfWeek](
      classOf[DayOfWeek],
      new KryoSerializer[DayOfWeek](classOf[DayOfWeek], new ExecutionConfig)))
  }

  @Test def testDecimalInferType(): Unit = {
    Assert.assertEquals(new DecimalType(7, 0), FlinkTypeSystem.inferIntDivType(5, 2, 4))
    Assert.assertEquals(new DecimalType(38, 5), FlinkTypeSystem.inferAggSumType(5))
    Assert.assertEquals(new DecimalType(false, 38, 6), FlinkTypeSystem.inferAggAvgType(5))
  }
}
