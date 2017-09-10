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

package org.apache.flink.table.api.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException, Types}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.utils.TestTableSourceWithTime
import org.apache.flink.types.Row
import org.junit.Test

class TableSourceValidationTest {

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithNullPath(): Unit = {
    CsvTableSource.builder()
      .field("myfield", Types.STRING)
      // should fail, path is not defined
      .build()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithDuplicateFieldName(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      .field("myfield", Types.STRING)
      // should fail, field name must no be duplicate
      .field("myfield", Types.INT)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testCsvTableSourceBuilderWithEmptyField(): Unit = {
    CsvTableSource.builder()
      .path("/path/to/csv")
      // should fail, field can be empty
      .build()
  }

  @Test(expected = classOf[TableException])
  def testNonExistingRowtimeField(): Unit = {
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount")
    )
    val ts = new TestTableSourceWithTime(
      Seq[Row](),
      rowType,
      "rTime",
      null
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // should fail because configured rowtime field is not in schema
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[TableException])
  def testInvalidTypeRowtimeField(): Unit = {
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount")
    )
    val ts = new TestTableSourceWithTime(
      Seq[Row](),
      rowType,
      "name",
      null
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // should fail because configured rowtime field is not of type Long or Timestamp
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[TableException])
  def testEmptyRowtimeField(): Unit = {
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount")
    )
    val ts = new TestTableSourceWithTime(
      Seq[Row](),
      rowType,
      "",
      null
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // should fail because configured rowtime field is empty
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[TableException])
  def testEmptyProctimeField(): Unit = {
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount")
    )
    val ts = new TestTableSourceWithTime(
      Seq[Row](),
      rowType,
      null,
      ""
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // should fail because configured proctime field is empty
    tEnv.registerTableSource("testTable", ts)
  }
}
