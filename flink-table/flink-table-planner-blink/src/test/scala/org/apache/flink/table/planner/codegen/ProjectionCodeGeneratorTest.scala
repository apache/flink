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

package org.apache.flink.table.planner.codegen

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.data.{DecimalData, GenericRowData, RowData, TimestampData}
import org.apache.flink.table.runtime.generated.Projection
import org.apache.flink.table.types.logical.{BigIntType, DecimalType, IntType, RowType, TimestampType}

import org.junit.{Assert, Test}

import scala.util.Random

/**
  * Test for [[ProjectionCodeGenerator]].
  */
class ProjectionCodeGeneratorTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testProjectionBinaryRow(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      RowType.of(new IntType(), new BigIntType()),
      RowType.of(new BigIntType(), new IntType()),
      Array(1, 0)
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]
    val row: BinaryRowData = projection.apply(GenericRowData.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionGenericRow(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      RowType.of(new IntType(), new BigIntType()),
      RowType.of(new BigIntType(), new IntType()),
      Array(1, 0),
      outClass = classOf[GenericRowData]
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, GenericRowData]]
    val row: GenericRowData = projection.apply(GenericRowData.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionManyField(): Unit = {
    val rowType = RowType.of((0 until 100).map(_ => new IntType()).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]
    val rnd = new Random()
    val input = GenericRowData.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  @Test
  def testProjectionManyFieldGenericRow(): Unit = {
    val rowType = RowType.of((0 until 100).map(_ => new IntType()).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray,
      outClass = classOf[GenericRowData]
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, GenericRowData]]
    val rnd = new Random()
    val input = GenericRowData.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  @Test
  def testProjectionBinaryRowWithVariableLengthData(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      RowType.of(
        new DecimalType(38, 0),
        new DecimalType(38, 0),
        new TimestampType(9)),
      RowType.of(
        new DecimalType(38, 0),
        new TimestampType(9),
        new DecimalType(38, 0)),
      Array(1, 2, 0)
    ).newInstance(classLoader).asInstanceOf[Projection[RowData, BinaryRowData]]

    val decimal = DecimalData.fromBigDecimal(java.math.BigDecimal.valueOf(123), 38, 0)
    val timestamp = TimestampData.fromEpochMillis(123)

    val expected: BinaryRowData = new BinaryRowData(3)
    val writer: BinaryRowWriter = new BinaryRowWriter(expected)
    writer.writeDecimal(0, decimal, 38)
    writer.writeTimestamp(1, timestamp, 9)
    writer.writeDecimal(2, decimal, 38)
    writer.complete()

    val actual: BinaryRowData = projection.apply(GenericRowData.of(decimal, decimal, timestamp))
    Assert.assertEquals(expected, actual)
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }
}
