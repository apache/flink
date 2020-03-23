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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo.LOCAL_DATE_TIME
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableSchema, Types}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.TestTableSourceWithTime
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils.unixTimestampToLocalDateTime
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

import org.junit.Test

import java.lang.{Integer => JInt, Long => JLong}

import scala.collection.JavaConversions._

class TableScanITCase extends BatchTestBase {

  @Test
  def testTableSourceWithoutTimeAttribute(): Unit = {
    val tableName = "MyTable"

    val tableSource = new StreamTableSource[Row]() {
      private val fieldNames: Array[String] = Array("name", "id", "value")
      private val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.LONG, Types.INT)

      override def isBounded: Boolean = true

      override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
        val data = Seq(
          row("Mary", new JLong(1L), new JInt(1)),
          row("Bob", new JLong(2L), new JInt(3))
        )
        val dataStream = execEnv.fromCollection(data).returns(getReturnType)
        dataStream.getTransformation.setMaxParallelism(1)
        dataStream
      }

      override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

      override def getTableSchema: TableSchema = new TableSchema(fieldNames, fieldTypes)
    }
    tEnv.registerTableSource(tableName, tableSource)

    checkResult(
      s"SELECT * from $tableName",
      Seq(
        row("Mary", 1L, 1),
        row("Bob", 2L, 3))
    )
  }

  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"
    val data = Seq("Mary", "Peter", "Bob", "Liz")
    val schema = new TableSchema(Array("name", "ptime"), Array(Types.STRING, Types.LOCAL_DATE_TIME))
    val returnType = Types.STRING

    val tableSource = new TestTableSourceWithTime(true, schema, returnType, data, null, "ptime")
    tEnv.registerTableSource(tableName, tableSource)

    checkResult(
      s"SELECT name FROM $tableName",
      Seq(
        row("Mary"),
        row("Peter"),
        row("Bob"),
        row("Liz"))
    )
  }

  @Test
  def testRowtimeTableSource(): Unit = {
    val tableName = "MyTable"
    val data = Seq(
      row("Mary", unixTimestampToLocalDateTime(1L), new JInt(10)),
      row("Bob", unixTimestampToLocalDateTime(2L), new JInt(20)),
      row("Mary", unixTimestampToLocalDateTime(2L), new JInt(30)),
      row("Liz", unixTimestampToLocalDateTime(2001L), new JInt(40)))

    val fieldNames = Array("name", "rtime", "amount")
    val schema = new TableSchema(fieldNames, Array(Types.STRING, LOCAL_DATE_TIME, Types.INT))
    val rowType = new RowTypeInfo(
      Array(Types.STRING, LOCAL_DATE_TIME, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      fieldNames)

    val tableSource = new TestTableSourceWithTime(true, schema, rowType, data, "rtime", null)
    tEnv.registerTableSource(tableName, tableSource)

    checkResult(
      s"SELECT * FROM $tableName",
      Seq(
        row("Mary", unixTimestampToLocalDateTime(1L), new JInt(10)),
        row("Mary", unixTimestampToLocalDateTime(2L), new JInt(30)),
        row("Bob", unixTimestampToLocalDateTime(2L), new JInt(20)),
        row("Liz", unixTimestampToLocalDateTime(2001L), new JInt(40)))
    )
  }

}
