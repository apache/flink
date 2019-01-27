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

import java.lang.{Integer => JInt, Long => JLong}
import org.apache.flink.table.api.types.{DataTypes, InternalType, RowType}
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, BinaryRowWriter, GenericRow}

import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.Test

/**
  * Test for [[EqualiserCodeGenerator]].
  */
class EqualiserCodeGeneratorTest {

  @Test
  def testEqualiser(): Unit = {
    val types = Array[InternalType](
      DataTypes.INT,
      new RowType(
        DataTypes.INT,
        DataTypes.STRING,
        new RowType(DataTypes.STRING, DataTypes.STRING)),
      DataTypes.LONG)
    val generator = new EqualiserCodeGenerator(types)
    val recordEqualiser = generator.generateRecordEqualiser("recordEqualiser")
      .newInstance(Thread.currentThread().getContextClassLoader)

    val rowLeft: GenericRow = GenericRow.of(
      1: JInt, GenericRow.of(2: JInt, "3", GenericRow.of("4", "5")), 6L: JLong)
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
    writer.writeString(0, c231)
    writer.writeString(1, c232)
    writer.complete()

    val c2Row = new BinaryRow(2)
    writer = new BinaryRowWriter(c2Row)
    writer.writeInt(0, c21)
    writer.writeString(1, c22)
    writer.writeBinaryRow(2, c23)
    writer.complete()

    val row = new BinaryRow(3)
    writer = new BinaryRowWriter(row)
    writer.writeInt(0, c1)
    writer.writeBinaryRow(1, c2Row)
    writer.writeLong(2, c3)
    writer.complete()
    row
  }
}


