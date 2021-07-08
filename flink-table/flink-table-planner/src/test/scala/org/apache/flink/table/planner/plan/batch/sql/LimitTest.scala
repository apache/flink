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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown

/**
 * The plan of following unit test in LimitTest.xml is a bit diffirent from LegacyLimitTest.xml.
 * Because the TestValuesTableSource has implemented [[SupportsProjectionPushDown]]
 * while the TestLegacyLimitableTableSource doesn't.
 * So the Calc has been pushed down to the scan.
 * 1.testFetchWithOffsetAndLimitSource
 * 2.testOrderByWithLimitSource
 * 3.testLimitWithLimitSource
 * 4.testLimitWithOffsetAndLimitSource
 * 5.testFetchWithLimitSource
 */
class LimitTest extends LegacyLimitTest {

  override def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val ddl =
    s"""
       |CREATE TABLE LimitTable (
       |  a int,
       |  b bigint,
       |  c string
       |) WITH (
       |  'connector' = 'values',
       |  'bounded' = 'true'
       |)
       """.stripMargin
    util.tableEnv.executeSql(ddl)
  }
}
