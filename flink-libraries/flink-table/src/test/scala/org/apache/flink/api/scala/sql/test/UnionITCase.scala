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

package org.apache.flink.api.scala.sql.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{TableException, TableEnvironment, Row}
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class UnionITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testUnion(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT c FROM t1 UNION ALL (SELECT c FROM t2)"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  //TODO: activate for EFFICIENT mode
  @Test
  def testUnionWithFilter(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    if (tEnv.getConfig.getEfficientTypeUsage) {
      return
    }

    val sqlQuery = "SELECT c FROM (" +
      "SELECT * FROM t1 UNION ALL (SELECT a, b, c FROM t2))" +
      "WHERE b < 2"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'd, 'c, 'e)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hi\n" + "Hallo\n"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  //TODO: activate for EFFICIENT mode
  @Test
  def testUnionWithAggregation(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    if (tEnv.getConfig.getEfficientTypeUsage) {
      return
    }

    val sqlQuery = "SELECT count(c) FROM (" +
      "SELECT * FROM t1 UNION ALL (SELECT a, b, c FROM t2))"

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    tEnv.registerDataSet("t1", ds1, 'a, 'b, 'c)
    tEnv.registerDataSet("t2", ds2, 'a, 'b, 'd, 'c, 'e)

    val result = tEnv.sql(sqlQuery)

    val expected = "18"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
