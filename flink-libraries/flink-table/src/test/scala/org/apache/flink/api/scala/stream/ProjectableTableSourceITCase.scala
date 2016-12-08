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

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.stream.utils.StreamITCase
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaExecEnv}
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.api.table.TableEnvironment
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala._
import org.apache.flink.api.table.sources.{ProjectableTableSource, StreamTableSource}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

class ProjectableTableSourceITCase extends StreamingMultipleProgramsTestBase {

  private val tableName = "MyTable"
  private var tableEnv: StreamTableEnvironment = _
  private var env: StreamExecutionEnvironment = _

  @Before
  def initTableEnv(): Unit = {
    env = StreamExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getTableEnvironment(env)
    tableEnv.registerTableSource(tableName, new TestProjectableTableSource)
  }

  @Test
  def testTableAPI(): Unit = {

    StreamITCase.testResults = mutable.MutableList()

    tableEnv
      .ingest(tableName)
      .where("amount < 4")
      .select("id, name")
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "0,Record_0", "1,Record_1", "2,Record_2", "3,Record_3", "16,Record_16",
      "17,Record_17", "18,Record_18", "19,Record_19", "32,Record_32")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSQL(): Unit = {

    StreamITCase.testResults = mutable.MutableList()

    tableEnv
      .sql(s"select id, name from $tableName where amount < 4 ")
      .toDataStream[Row]
      .addSink(new StreamITCase.StringSink)

    env.execute()

    val expected = mutable.MutableList(
      "0,Record_0", "1,Record_1", "2,Record_2", "3,Record_3", "16,Record_16",
      "17,Record_17", "18,Record_18", "19,Record_19", "32,Record_32")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

class TestProjectableTableSource(
  fieldTypes: Array[TypeInformation[_]],
  fieldNames: Array[String])
  extends StreamTableSource[Row] with ProjectableTableSource[Row] {

  def this() = this(
    fieldTypes = Array(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO),
    fieldNames = Array[String]("name", "id", "amount", "price")
  )

  /** Returns the data of the table as a [[DataStream]]. */
  override def getDataStream(execEnv: JavaExecEnv): JavaStream[Row] = {
    execEnv.fromCollection(generateDynamicCollection(33, fieldNames).asJava, getReturnType)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = fieldNames

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes: _*)

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = fieldNames.length

  override def projectFields(fields: Array[Int]): TestProjectableTableSource = {
    val projectedFieldTypes = new Array[TypeInformation[_]](fields.length)
    val projectedFieldNames = new Array[String](fields.length)

    fields.zipWithIndex.foreach(f => {
      projectedFieldTypes(f._2) = fieldTypes(f._1)
      projectedFieldNames(f._2) = fieldNames(f._1)
    })
    new TestProjectableTableSource(projectedFieldTypes, projectedFieldNames)
  }

  private def generateDynamicCollection(num: Int, fieldNames: Array[String]): Seq[Row] = {
    for {cnt <- 0 until num}
      yield {
        val row = new Row(fieldNames.length)
        fieldNames.zipWithIndex.foreach(
          f =>
            f._1 match {
              case "name" =>
                row.setField(f._2, "Record_" + cnt)
              case "id" =>
                row.setField(f._2, cnt.toLong)
              case "amount" =>
                row.setField(f._2, cnt.toInt % 16)
              case "price" =>
                row.setField(f._2, cnt.toDouble / 3)
              case _ =>
                throw new IllegalArgumentException(s"unknown field name $f._1")
            }
        )
        row
      }
  }
}
