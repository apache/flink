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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.catalog.TableIdentifier
import org.apache.flink.table.utils.CommonTestData
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ExternalCatalogITCase(
    configMode: TableConfigMode)
    extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testSQL(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)
    val sqlQuery = "SELECT * FROM test.db1.tb1 UNION ALL " +
        "(SELECT d, e, g FROM test.db2.tb2 WHERE d < 3)"
    val results = tEnv.sql(sqlQuery).collect()

    val expected = Seq(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "1,1,Hallo",
      "2,2,Hallo Welt",
      "2,3,Hallo Welt wie").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTableAPI(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    tEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog)
    val table1 = tEnv.scan("test", "db1", "tb1")
    val table2 = tEnv.scan("test", "db2", "tb2")
    val results = table2.where("d < 3")
        .select("d, e, g")
        .unionAll(table1)
        .collect()

    val expected = Seq(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world",
      "1,1,Hallo",
      "2,2,Hallo Welt",
      "2,3,Hallo Welt wie").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
