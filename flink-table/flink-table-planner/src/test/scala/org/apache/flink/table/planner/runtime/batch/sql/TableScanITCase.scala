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

import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.TestTableSourceSinks.createWithoutTimeAttributesTableSource
import org.apache.flink.table.utils.DateTimeUtils.toLocalDateTime

import org.junit.jupiter.api.Test

import java.lang.{Integer => JInt}

class TableScanITCase extends BatchTestBase {

  @Test
  def testTableSourceWithoutTimeAttribute(): Unit = {
    val tableName = "MyTable"
    createWithoutTimeAttributesTableSource(tEnv, tableName)
    checkResult(
      s"SELECT * from $tableName",
      Seq(row("Mary", 1L, 1), row("Bob", 2L, 3))
    )
  }

  @Test
  def testProctimeTableSource(): Unit = {
    val tableName = "MyTable"
    val dataId =
      TestValuesTableFactory.registerData(Seq(row("Mary"), row("Peter"), row("Bob"), row("Liz")))
    tEnv.executeSql(s"""
                       |create table $tableName (
                       |  name string,
                       |  ptime as proctime()
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'true',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)

    checkResult(
      s"SELECT name, CHAR_LENGTH(DATE_FORMAT(ptime, 'yyyy-MM-dd HH:mm')) FROM $tableName",
      Seq(row("Mary", 16), row("Peter", 16), row("Bob", 16), row("Liz", 16))
    )
  }

  @Test
  def testRowtimeTableSource(): Unit = {
    val tableName = "MyTable"
    val dataId =
      TestValuesTableFactory.registerData(
        Seq(
          row("Mary", toLocalDateTime(1L), new JInt(10)),
          row("Bob", toLocalDateTime(2L), new JInt(20)),
          row("Mary", toLocalDateTime(2L), new JInt(30)),
          row("Liz", toLocalDateTime(2001L), new JInt(40))
        ))
    tEnv.executeSql(s"""
                       |create table $tableName (
                       |  name string,
                       |  rtime timestamp(3),
                       |  amount int,
                       |  watermark for rtime as rtime
                       |) with (
                       |  'connector' = 'values',
                       |  'bounded' = 'true',
                       |  'data-id' = '$dataId'
                       |)
                       |""".stripMargin)

    checkResult(
      s"SELECT * FROM $tableName",
      Seq(
        row("Mary", toLocalDateTime(1L), new JInt(10)),
        row("Mary", toLocalDateTime(2L), new JInt(30)),
        row("Bob", toLocalDateTime(2L), new JInt(20)),
        row("Liz", toLocalDateTime(2001L), new JInt(40))
      )
    )
  }

}
