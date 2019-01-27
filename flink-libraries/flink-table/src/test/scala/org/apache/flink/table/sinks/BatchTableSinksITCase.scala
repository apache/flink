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

package org.apache.flink.table.sinks

import java.lang.{Boolean => JBoolean}

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.dataformat.BinaryString.fromString
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.binaryRow
import org.apache.flink.table.runtime.utils._
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row

import org.junit.Before
import org.junit._
import org.junit.Assert._

import scala.collection.{Seq, mutable}

class BatchTableSinksITCase extends BatchTestBase {

  val dataType = new BaseRowTypeInfo(
    INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)

  val data = Seq(
    binaryRow(dataType, 1, 1L, fromString("Hi")),
    binaryRow(dataType, 2, 2L, fromString("Hello")),
    binaryRow(dataType, 3, 2L, fromString("Hello world")),
    binaryRow(dataType, 4, 3L, fromString("Hello world, how are you?"))
  )

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    tEnv.registerCollection("sTable", data, dataType, 'a, 'b, 'c)
    tEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashAgg")
  }

  @Test
  def testAppendSinkOnAppendTable(): Unit = {
    val tableSink = new TestingAppendTableSink
    tEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    tEnv.execute()
    val expected = List (
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "4,3,Hello world, how are you?"
    ).sorted

    assertEquals(expected, tableSink.getResults.sorted)
  }

  @Test
  def testRetractSinkTable(): Unit = {
    val tableSink = new TestingRetractTableSink
    tEnv.sqlQuery("select a, b, c from sTable").writeToSink(tableSink)
    tEnv.execute()
    val expected = List (
      "(true,1,1,Hi)",
      "(true,2,2,Hello)",
      "(true,3,2,Hello world)",
      "(true,4,3,Hello world, how are you?)"
    ).sorted

    assertEquals(expected, tableSink.getResults.sorted)
  }

  @Test
  def testSumCountRetractSinkTableSum(): Unit = {
    val tableSink = new TestingRetractTableSink
    tEnv.sqlQuery("select sum(a), count(1) from sTable group by b").writeToSink(tableSink)
    tEnv.execute()
    val expected = List (
      "(true,1,1)",
      "(true,5,2)",
      "(true,4,1)"
    ).sorted

    assertEquals(expected, tableSink.getResults.sorted)
  }

  @Test
  def testPartitinalSink(): Unit = {
    val data = StreamTestData.get3TupleData
    val t = tEnv.fromCollection(data, 'id, 'num, 'name)
    val testingRetractSink = new TestingRetractTableSink
    val globalPartitionalResults = mutable.HashMap.empty[Int, mutable.HashSet[String]]
    testingRetractSink.setPartitionField("id")
    testingRetractSink.setPartitionalOutput(
      new TestPartitionalOutputFormat(
        globalPartitionalResults, (value: JTuple2[JBoolean, Row]) => {
          value.f1.getField(0).toString
        }
      ))
    t.groupBy("num").select("id.sum as id, num").writeToSink(testingRetractSink)
    tEnv.execute()
  }
}
