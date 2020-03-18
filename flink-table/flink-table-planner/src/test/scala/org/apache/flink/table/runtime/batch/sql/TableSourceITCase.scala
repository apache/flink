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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{CommonTestData, TableProgramsCollectionTestBase}
import org.apache.flink.table.runtime.utils.TableProgramsTestBase.TableConfigMode
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
    val tEnv = BatchTableEnvironment.create(env, config)

    tEnv.registerTableSource("csvTable", csvTable)
    val results = tEnv.sqlQuery(
      "SELECT id, `first`, `last`, score FROM csvTable").collect()

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
  def testCsvTableSourceWithEmptyColumn(): Unit = {

    val csvTable = CommonTestData.getCsvTableSourceWithEmptyColumn

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env, config)

    tEnv.registerTableSource("csvTable", csvTable)
    val results = tEnv.sqlQuery(
      "SELECT id, `first`, `last`, score FROM csvTable").collect()

    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,null",
      "null,Leonard,null,null").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNested(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env, config)
    val nestedTable = CommonTestData.getNestedTableSource

    tableEnv.registerTableSource("NestedPersons", nestedTable)

    val result = tableEnv.sqlQuery("SELECT NestedPersons.firstName, NestedPersons.lastName," +
        "NestedPersons.address.street, NestedPersons.address.city AS city " +
        "FROM NestedPersons " +
        "WHERE NestedPersons.address.city LIKE 'Dublin'").collect()

    val expected = "Bob,Taylor,Pearse Street,Dublin"

    TestBaseUtils.compareResultAsText(result.asJava, expected)
  }

}
