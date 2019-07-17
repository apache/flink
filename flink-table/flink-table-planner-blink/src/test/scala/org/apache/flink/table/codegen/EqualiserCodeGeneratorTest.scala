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

import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, BinaryRowWriter, BinaryString, GenericRow}
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.typeutils.BaseRowSerializer

/**
  * Test for [[EqualiserCodeGenerator]].
  */
class EqualiserCodeGeneratorTest {

  @Test
  def testEqualiser(): Unit = {
    val types = Array[LogicalType](
      DataTypes.INT.getLogicalType,
      DataTypes.ROW(
        DataTypes.FIELD("f0", DataTypes.INT),
        DataTypes.FIELD("f1", DataTypes.STRING),
        DataTypes.FIELD("f2", DataTypes.ROW(
          DataTypes.FIELD("f20", DataTypes.STRING),
          DataTypes.FIELD("f21", DataTypes.STRING)
        ))).getLogicalType,
      DataTypes.BIGINT.getLogicalType)
    val generator = new EqualiserCodeGenerator(types)
    val recordEqualiser = generator.generateRecordEqualiser("recordEqualiser")
      .newInstance(Thread.currentThread().getContextClassLoader)

    val rowLeft: GenericRow = GenericRow.of(
      1: JInt,
      GenericRow.of(
        2: JInt,
        BinaryString.fromString("3"),
        GenericRow.of(
          BinaryString.fromString("4"), BinaryString.fromString("5"))), 6L: JLong)
    val rowRight: BaseRow = newBinaryRow(1: JInt, 2, "3", "4", "5", 6L)
    assertTrue(recordEqualiser.equals(rowLeft, rowRight))

    rowLeft.setHeader(1)
    rowRight.setHeader(0)
    assertFalse(recordEqualiser.equals(rowLeft, rowRight))
    assertTrue(recordEqualiser.equalsWithoutHeader(rowLeft, rowRight))
  }

  def newBinaryRow(
      c1: Int, c21: Int, c22: String, c231: String, c232: String, c3: Long): BinaryRow = {
    val c23 = new BinaryRow(2)
    var writer = new BinaryRowWriter(c23)
    writer.writeString(0, BinaryString.fromString(c231))
    writer.writeString(1, BinaryString.fromString(c232))
    writer.complete()

    val c2Row = new BinaryRow(2)
    val c2Serializer = new BaseRowSerializer(
      new ExecutionConfig(), DataTypes.STRING.getLogicalType, DataTypes.STRING.getLogicalType)
    writer = new BinaryRowWriter(c2Row)
    writer.writeInt(0, c21)
    writer.writeString(1, BinaryString.fromString(c22))
    writer.writeRow(2, c23, c2Serializer)
    writer.complete()

    val row = new BinaryRow(3)
    writer = new BinaryRowWriter(row)
    val serializer = new BaseRowSerializer(
      new ExecutionConfig(),
      DataTypes.INT.getLogicalType,
      DataTypes.STRING.getLogicalType,
      DataTypes.ROW(
        DataTypes.FIELD("f20", DataTypes.STRING),
        DataTypes.FIELD("f21", DataTypes.STRING)).getLogicalType)
    writer.writeInt(0, c1)
    writer.writeRow(1, c2Row, serializer)
    writer.writeLong(2, c3)
    writer.complete()
    row
  }
}
