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

import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class UnionLookupJoinITCase extends StreamingTestBase {

  @BeforeEach
  def beforeEach(): Unit = {
    super.before()
    val streamTableId = TestValuesTableFactory.registerData(Seq(row(1L, "Alice"), row(2L, "Bob")))
    val streamTableDDL =
      s"""
         | CREATE TABLE `stream` (
         |   `id` BIGINT,
         |   `name` STRING,
         |   `txn_time` AS PROCTIME(),
         |   PRIMARY KEY (`id`) NOT ENFORCED
         | ) WITH (
         |   'connector' = 'values',
         |   'data-id' = '$streamTableId'
         | )
         |""".stripMargin
    tEnv.executeSql(streamTableDDL)

    val dimTableId = TestValuesTableFactory.registerData(Seq(row(1L, "OK"), row(2L, "OK")))
    val dimTableDDL =
      s"""
         | CREATE TABLE `dim` (
         |   `id` BIGINT,
         |   `status` STRING,
         |   PRIMARY KEY (`id`) NOT ENFORCED
         | ) WITH (
         |   'connector' = 'values',
         |   'filterable-fields' = 'id;status',
         |   'data-id' = '$dimTableId'
         | )
         |""".stripMargin
    tEnv.executeSql(dimTableDDL)
  }

  @ParameterizedTest
  @CsvSource(Array[String]("OK, NOT_EXISTS", "NOT_EXISTS, OK"))
  def testUnionTemporalJoinWithFilterPushdownSource(
      firstFilterValue: String,
      secondFilterValue: String): Unit = {
    val query = getUnionQuery(firstFilterValue, secondFilterValue);
    val expectedData = Seq("1,Alice,OK", "2,Bob,OK")

    val result = tEnv.sqlQuery(query).toDataStream
    val sink = new TestingAppendSink()
    result.addSink(sink)

    env.execute()
    assertThat(sink.getAppendResults.sorted).isEqualTo(expectedData.sorted)
  }

  private def getUnionQuery(firstFilterValue: String, secondFilterValue: String): String = {
    s"""
       | SELECT s.id, s.name, d.status
       | FROM `stream` AS `s` INNER JOIN `dim` FOR SYSTEM_TIME AS OF `s`.`txn_time` AS `d`
       | ON `s`.`id` = `d`.`id`
       | WHERE `d`.`status` = '$firstFilterValue'
       | UNION ALL
       | SELECT s.id, s.name, d.status
       | FROM `stream` AS `s` INNER JOIN `dim` FOR SYSTEM_TIME AS OF `s`.`txn_time` AS `d`
       | ON `s`.`id` = `d`.`id`
       | WHERE `d`.`status` = '$secondFilterValue'
       |""".stripMargin
  }

}
