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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.planner.utils.{TestPreserveWMTableSource, TestTableSourceWithTime}
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import org.junit.Assert._
import org.junit.Test

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConversions._

class TableScanITCase extends StreamingTestBase {

  @Test
  def testTableSourceWithoutTimeAttribute(): Unit = {
    val tableName = "MyTable"

    val tableSource = new StreamTableSource[Row]() {
      private val fieldNames: Array[String] = Array("name", "id", "value")
      private val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.LONG, Types.INT)

      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
        val data = Seq(
          Row.of("Mary", new JLong(1L), new JInt(1)),
          Row.of("Bob", new JLong(2L), new JInt(3))
        )
        val dataStream = execEnv.fromCollection(data).returns(getReturnType)
        dataStream.getTransformation.setMaxParallelism(1)
        dataStream
      }

      override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

      override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
    }
    tEnv.registerTableSource(tableName, tableSource)
    val sqlQuery = s"SELECT * from $tableName"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("Mary,1,1", "Bob,2,3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"

    val data = Seq("Mary", "Peter", "Bob", "Liz")

    val schema = new TableSchema(Array("name", "ptime"), Array(Types.STRING, Types.LOCAL_DATE_TIME))
    val returnType = Types.STRING

    val tableSource = new TestTableSourceWithTime(false, schema, returnType, data, null, "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    val sqlQuery = s"SELECT name FROM $tableName"
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink
    result.addSink(sink)
    env.execute()

    val expected = Seq("Mary", "Peter", "Bob", "Liz")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testRowtimeTableSourcePreserveWatermarks(): Unit = {
    val tableName = "MyTable"

    // rows with timestamps and watermarks
    val data = Seq(
      Right(1L),
      Left(5L, Row.of(new JInt(1), new JLong(5), "A")),
      Left(2L, Row.of(new JInt(2), new JLong(1), "B")),
      Right(10L),
      Left(8L, Row.of(new JInt(6), new JLong(8), "C")),
      Right(20L),
      Left(21L, Row.of(new JInt(6), new JLong(21), "D")),
      Right(30L)
    )

    val fieldNames = Array("id", "rtime", "name")
    val schema = new TableSchema(fieldNames, Array(Types.INT, Types.LOCAL_DATE_TIME, Types.STRING))
    val rowType = new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestPreserveWMTableSource(schema, rowType, data, "rtime")
    tEnv.registerTableSource(tableName, tableSource)
    val sqlQuery = s"SELECT id, name FROM $tableName"
    val sink = new TestingAppendSink

    tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
      // append current watermark to each row to verify that original watermarks were preserved
      .process(new ProcessFunction[Row, Row] {

      override def processElement(
          value: Row,
          ctx: ProcessFunction[Row, Row]#Context,
          out: Collector[Row]): Unit = {
        val res = new Row(3)
        res.setField(0, value.getField(0))
        res.setField(1, value.getField(1))
        res.setField(2, ctx.timerService().currentWatermark())
        out.collect(res)
      }
    }).addSink(sink)
    env.execute()

    val expected = Seq("1,A,1", "2,B,1", "6,C,10", "6,D,20")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
