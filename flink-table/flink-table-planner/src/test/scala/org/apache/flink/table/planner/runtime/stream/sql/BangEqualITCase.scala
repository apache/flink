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

import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class BangEqualITCase extends StreamingTestBase {

  @Test
  def testBangEqual(): Unit = {
    TestCollectionTableFactory.reset()

    val sourceData = List(
      rowOf(1, "1"),
      rowOf(2, "2"),
      rowOf(3, "3"),
      rowOf(4, "4"),
      rowOf(5, "5"))

    val expectedData = List(
      rowOf(1, "1"),
      rowOf(3, "3"),
      rowOf(4, "4"),
      rowOf(5, "5"))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE StreamBangEqualResult(
        |  a int,
        |  b varchar
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val queryWithBangEqual = "SELECT a,b FROM T1 WHERE a != 2"

    tEnv.executeSql(sourceDDL)
    tEnv.executeSql(sinkDDL)
    tEnv.sqlQuery(queryWithBangEqual).executeInsert("StreamBangEqualResult").await()

    implicit def rowOrdering: Ordering[Row] = Ordering.by((r: Row) => {
      r.getField(0).toString
    })

    assertEquals(expectedData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }
}
