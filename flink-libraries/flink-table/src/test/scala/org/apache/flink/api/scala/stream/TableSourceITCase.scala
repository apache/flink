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

package org.apache.flink.api.scala.stream

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.StreamITCase
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.sources.{CsvTableSource, StreamTableSource}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class TableSourceITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testStreamTableSourceTableAPI(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    tEnv.registerTableSource("MyTestTable", new TestStreamTableSource(33))
    tEnv.ingest("MyTestTable")
      .where('amount < 4)
      .select('amount * 'id, 'name)
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "0,Record_0", "0,Record_16", "0,Record_32",
      "1,Record_1", "17,Record_17", "36,Record_18",
      "4,Record_2", "57,Record_19", "9,Record_3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testStreamTableSourceSQL(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    tEnv.registerTableSource("MyTestTable", new TestStreamTableSource(33))
    tEnv.sql(
      "SELECT STREAM amount * id, name FROM MyTestTable WHERE amount < 4")
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "0,Record_0", "0,Record_16", "0,Record_32",
      "1,Record_1", "17,Record_17", "36,Record_18",
      "4,Record_2", "57,Record_19", "9,Record_3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testCsvTableSource(): Unit = {

    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFile = File.createTempFile("csv-test", "tmp")
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8")
    tmpWriter.write(csvRecords.mkString("$"))
    tmpWriter.close()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val csvTable = new CsvTableSource(
      tempFile.getAbsolutePath,
      Array("first", "id", "score", "last"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "#",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )

    tEnv.registerTableSource("csvTable", csvTable)
    tEnv.sql(
      "SELECT STREAM last, score, id FROM csvTable WHERE id < 4 ")
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "Smith,12.3,1",
      "Taylor,45.6,2",
      "Miller,7.89,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}


class TestStreamTableSource(val numRecords: Int) extends StreamTableSource[Row] {

  val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO
  )

  /** Returns the data of the table as a [[DataStream]]. */
  override def getDataStream(execEnv: environment.StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.addSource(new GeneratingSourceFunction(numRecords), getReturnType).setParallelism(1)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = Array("name", "id", "amount")

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes)

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = 3
}

class GeneratingSourceFunction(val num: Long) extends SourceFunction[Row] {

  var running = true

  override def run(ctx: SourceContext[Row]): Unit = {
    var cnt = 0L
    while(running && cnt < num) {
      val out = new Row(3)
      out.setField(0, s"Record_$cnt")
      out.setField(1, cnt)
      out.setField(2, (cnt % 16).toInt)

      ctx.collect(out)
      cnt += 1
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
