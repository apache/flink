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

import java.util
import java.util.Collections

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.api.{TableEnvironment, TableSchema, Types, ValidationException}
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps
import org.apache.flink.table.util.TestTableSourceWithTime
import org.apache.flink.types.Row
import org.junit.Test

class TableSourceValidationTest {

  @Test(expected = classOf[ValidationException])
  def testUnresolvedSchemaField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount", "value"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT, DataTypes.DOUBLE))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount"))
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row]())

    // should fail because schema field "value" cannot be resolved in result type
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testNonMatchingFieldTypes(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount"),
      Array(DataTypes.LONG, DataTypes.INT, DataTypes.INT))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount"))
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row]())

    // should fail because types of "name" fields are different
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testMappingToUnknownField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.DOUBLE))
    val rowType = new RowTypeInfo(Types.LONG, Types.STRING, Types.DOUBLE)
    val mapping = Map("id" -> "f3", "name" -> "f1", "amount" -> "f2")
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row](), mapping = mapping)

    // should fail because mapping maps field "id" to unknown field
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testMappingWithInvalidFieldType(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.DOUBLE))
    val rowType = new RowTypeInfo(Types.LONG, Types.STRING, Types.INT)
    val mapping = Map("id" -> "f0", "name" -> "f1", "amount" -> "f2")
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row](), mapping = mapping)

    // should fail because mapping maps fields with different types
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testNonTimestampProctimeField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount", "ptime"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT, DataTypes.LONG))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "amount"))
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row](), proctime = "ptime")

    // should fail because processing time field has invalid type
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testNonTimestampRowtimeField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount", "rtime"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT, DataTypes.LONG))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.LONG, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "rtime", "amount"))
    val ts = new TestTableSourceWithTime(schema, rowType, Seq[Row](), rowtime = "rtime")

    // should fail because rowtime field has invalid type
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testFieldRowtimeAndProctime(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount", "time"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT, DataTypes.TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.LONG, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "time", "amount"))
    val ts =
      new TestTableSourceWithTime(schema, rowType, Seq[Row](), rowtime = "time", proctime = "time")

    // should fail because rowtime field has invalid type
    tEnv.registerTableSource("testTable", ts)
  }

  @Test(expected = classOf[ValidationException])
  def testUnknownTimestampExtractorArgField(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val schema = new TableSchema(
      Array("id", "name", "amount", "rtime"),
      Array(DataTypes.LONG, DataTypes.STRING, DataTypes.INT, DataTypes.TIMESTAMP))
    val rowType = new RowTypeInfo(
      Array(Types.LONG, Types.STRING, Types.LONG, Types.INT)
        .asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "name", "rtime", "amount"))
    val ts =
      new TestTableSourceWithTime(schema, rowType, Seq[Row]()) {

        override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
          Collections.singletonList(new RowtimeAttributeDescriptor(
            "rtime",
            new ExistingField("doesNotExist"),
            new AscendingTimestamps))
        }
    }

    // should fail because timestamp extractor argument field does not exist
    tEnv.registerTableSource("testTable", ts)
  }
}
