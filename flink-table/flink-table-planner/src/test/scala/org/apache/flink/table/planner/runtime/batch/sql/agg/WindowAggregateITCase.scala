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

package org.apache.flink.table.planner.runtime.batch.sql.agg

import java.time.LocalDateTime

import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.types.Row
import org.junit.Test

class WindowAggregateITCase extends BatchTestBase {

  @Test
  def testLocalGlobalWindowAggregateWithoutGroupingAndNamedProperties(): Unit = {
    val data: Seq[Row] = Seq(
      row(1, LocalDateTime.of(2021, 7, 26, 0, 0, 0)),
      row(2, LocalDateTime.of(2021, 7, 26, 0, 0, 3)),
      row(3, LocalDateTime.of(2021, 7, 26, 0, 0, 6)),
      row(4, LocalDateTime.of(2021, 7, 26, 0, 0, 4)),
      row(5, LocalDateTime.of(2021, 7, 26, 0, 0, 5)),
      row(6, LocalDateTime.of(2021, 7, 26, 0, 0, 8)),
      row(7, LocalDateTime.of(2021, 7, 26, 0, 0, 10)))
    val dataId = TestValuesTableFactory.registerData(data)
    val ddl =
      s"""
         |CREATE TABLE MyTable (
         |  a INT,
         |  ts TIMESTAMP
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$dataId',
         |  'bounded' = 'true'
         |)
         |""".stripMargin

    tEnv.executeSql(ddl)
    checkResult(
      """
        |SELECT sum(a) FROM MyTable
        |GROUP BY
        |TUMBLE(ts, interval '5' seconds)
        |""".stripMargin,
      Seq(row(14), row(7), row(7)))
  }
}
