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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.FileSystemITCaseBase
import org.apache.flink.table.planner.runtime.utils.{AbstractExactlyOnceSink, StreamingTestBase, TestingAppendSink, TestSinkUtil}
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.extension.ExtendWith

import scala.collection.{mutable, Seq}

/** Streaming [[FileSystemITCaseBase]]. */
@ExtendWith(Array(classOf[NoOpTestExtension]))
abstract class StreamFileSystemITCaseBase extends StreamingTestBase with FileSystemITCaseBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()
    super.open()
  }

  override def tableEnv: TableEnvironment = {
    tEnv
  }

  override def check(sqlQuery: String, expectedResult: Seq[Row]): Unit = {
    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    val sink = new TestingAppendSink()
    result.addSink(sink)
    env.execute()

    assertThat(expectedResult.map(TestSinkUtil.rowToString(_)).sorted)
      .isEqualTo(sink.getAppendResults.sorted)
  }

  override def checkPredicate(sqlQuery: String, checkFunc: Row => Unit): Unit = {
    val result = tEnv.sqlQuery(sqlQuery).toDataStream
    val sinkResults = new mutable.MutableList[Row]

    val sink = new AbstractExactlyOnceSink[Row] {
      override def invoke(value: Row, context: SinkFunction.Context): Unit =
        sinkResults += value
    }
    result.addSink(sink)
    env.execute()

    try {
      sinkResults.foreach(checkFunc)
    } catch {
      case e: AssertionError =>
        throw new AssertionError(
          s"""
             |Results do not match for query:
             |  $sqlQuery
       """.stripMargin,
          e)
    }
  }

  // Streaming mode not support overwrite
  @Test
  override def testInsertOverwrite(): Unit = {}
}
