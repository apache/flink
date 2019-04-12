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

package org.apache.flink.table.codegen

import org.apache.flink.table.`type`.{InternalTypes, RowType}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, GenericRow}
import org.apache.flink.table.generated.Projection

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
      new RowType(InternalTypes.INT, InternalTypes.LONG),
      new RowType(InternalTypes.LONG, InternalTypes.INT),
      Array(1, 0)
    ).newInstance(classLoader).asInstanceOf[Projection[BaseRow, BinaryRow]]
    val row: BinaryRow = projection.apply(GenericRow.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionGenericRow(): Unit = {
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      new RowType(InternalTypes.INT, InternalTypes.LONG),
      new RowType(InternalTypes.LONG, InternalTypes.INT),
      Array(1, 0),
      outClass = classOf[GenericRow]
    ).newInstance(classLoader).asInstanceOf[Projection[BaseRow, GenericRow]]
    val row: GenericRow = projection.apply(GenericRow.of(ji(5), jl(8)))
    Assert.assertEquals(5, row.getInt(1))
    Assert.assertEquals(8, row.getLong(0))
  }

  @Test
  def testProjectionManyField(): Unit = {
    val rowType = new RowType((0 until 100).map(_ => InternalTypes.INT).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray
    ).newInstance(classLoader).asInstanceOf[Projection[BaseRow, BinaryRow]]
    val rnd = new Random()
    val input = GenericRow.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  @Test
  def testProjectionManyFieldGenericRow(): Unit = {
    val rowType = new RowType((0 until 100).map(_ => InternalTypes.INT).toArray: _*)
    val projection = ProjectionCodeGenerator.generateProjection(
      new CodeGeneratorContext(new TableConfig),
      "name",
      rowType,
      rowType,
      (0 until 100).toArray,
      outClass = classOf[GenericRow]
    ).newInstance(classLoader).asInstanceOf[Projection[BaseRow, GenericRow]]
    val rnd = new Random()
    val input = GenericRow.of((0 until 100).map(_ => ji(rnd.nextInt())).toArray: _*)
    val row = projection.apply(input)
    for (i <- 0 until 100) {
      Assert.assertEquals(input.getInt(i), row.getInt(i))
    }
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }
}
