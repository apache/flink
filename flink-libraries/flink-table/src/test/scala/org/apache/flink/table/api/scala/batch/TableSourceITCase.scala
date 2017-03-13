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

package org.apache.flink.table.api.scala.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.utils.{CommonTestData, TestFilterableTableSource}
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableSourceITCase(
    configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testCsvTableSource(): Unit = {

    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerTableSource("csvTable", csvTable)
    val results = tEnv.sql(
      "SELECT id, first, last, score FROM csvTable").collect()

    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCsvTableSourceWithProjection(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv
      .scan("csvTable")
      .where('score < 20)
      .select('last, 'id.floor(), 'score * 2)
      .collect()

    val expected = Seq(
      "Smith,1,24.6",
      "Miller,3,15.78",
      "Smith,4,0.24",
      "Miller,6,13.56",
      "Williams,8,4.68").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNestedBatchTableSourceSQL(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val nestedTable = CommonTestData.getNestedTableSource

    tableEnv.registerTableSource("NestedPersons", nestedTable)

    val result = tableEnv.sql("SELECT NestedPersons.firstName, NestedPersons.lastName," +
        "NestedPersons.address.street, NestedPersons.address.city AS city " +
        "FROM NestedPersons " +
        "WHERE NestedPersons.address.city LIKE 'Dublin'").collect()

    val expected = "Bob,Taylor,Pearse Street,Dublin"

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    val tableName = "MyTable"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    tableEnv.registerTableSource(tableName, new TestFilterableTableSource)
    val results = tableEnv
      .scan(tableName)
      .where("amount > 4 && price < 9")
      .select("id, name")
      .collect()

    val expected = Seq(
      "5,Record_5", "6,Record_6", "7,Record_7", "8,Record_8").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
