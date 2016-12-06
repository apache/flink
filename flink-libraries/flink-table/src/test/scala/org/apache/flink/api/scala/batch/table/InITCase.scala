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
package org.apache.flink.api.scala.batch.table


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{Row, Table, TableEnvironment, ValidationException}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class InITCase(
                mode: TestExecutionMode,
                configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {


  @Test
  def testSqlInOperator(): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T", ds1)

    val sqlQuery = "SELECT a, c FROM T WHERE b IN (SELECT b FROM T WHERE b = 6 OR b = 7)";
    val result = tEnv.sql(sqlQuery);
    result.toDataSet[Row].print()
  }

  @Test
  def testInSubqueryCorrelated(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.where('a === 6).select('c)
    val subqueryIn = ds1.where('c.in(subquery))


    val result = subqueryIn.toDataSet[Row].collect()
    val expected = "6,3,Luke Skywalker\n"

    TestBaseUtils.compareResultAsText(result.asJava, expected)

  }

  @Test
  def testInSubquery(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f)
    val ds2 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.where('d === 1).select('e)
    val subqueryIn = ds2.where('b.in(subquery))



    val resultsSimple = subqueryIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n"

    TestBaseUtils.compareResultAsText(resultsSimple.asJava, expected)

  }

}
