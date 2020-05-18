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

package org.apache.flink.table.runtime.harness

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

import org.junit.Test

import java.time.{Instant, ZoneId}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

class MatchHarnessTest extends HarnessTestBase {

  import RecordBuilder._

  @Test
  def testAccessingProctime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(
      env, EnvironmentSettings.newInstance().useOldPlanner().build())

    val data = new mutable.MutableList[(Int, String)]
    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT *
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    MATCH_PROCTIME() as proctime,
         |    HOUR(MATCH_PROCTIME()) as currentHour
         |  PATTERN (A)
         |  DEFINE
         |    A AS A.name LIKE '%a%'
         |) AS T
         |""".stripMargin

    val harness = createHarnessTester[Byte, Row, CRow](
      tEnv.sqlQuery(sqlQuery).toAppendStream[Row],
      "GlobalCepOperator")
    harness.open()

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    val now = Instant.ofEpochSecond(1000000)
    harness.setProcessingTime(now.toEpochMilli)

    harness.processElement(row(Int.box(1), "a").asRecord())
    val currentHour = now.atZone(ZoneId.of("GMT")).getHour
    // MATCH_PROCTIME is not materialized, therefore it is null, HOUR(MATCH_PROCTIME) is
    // materialized
    expectedOutput.add(cRow(null, Long.box(currentHour)).asRecord())

    verify(expectedOutput, harness.getOutput)
  }

  private class RecordBuilder[T](record: T) {
    def asRecord() : StreamRecord[T] = {
      new StreamRecord[T](record)
    }
  }

  private object RecordBuilder {
    def row(values: AnyRef*) : RecordBuilder[Row] = {
      new RecordBuilder[Row](Row.of(values : _*))
    }

    def cRow(values: AnyRef*) : RecordBuilder[CRow] = {
      new RecordBuilder[CRow](CRow(values : _*))
    }
  }
}
