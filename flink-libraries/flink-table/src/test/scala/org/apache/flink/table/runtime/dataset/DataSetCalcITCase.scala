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

package org.apache.flink.table.runtime.dataset

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsClusterTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.expressions.utils.{RichFunc1, RichFunc2, RichFunc3}
import org.apache.flink.table.utils._
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class DataSetCalcITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsClusterTestBase(mode, configMode) {

  @Test
  def testUserDefinedScalarFunctionWithParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='ABC#Hello'"

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUserDefinedScalarFunctionWithDistributedCache(): Unit = {
    val words = "Hello\nWord"
    val filePath = UserDefinedFunctionTestUtils.writeCacheFile("test_words", words)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile(filePath, "words")
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc3", new RichFunc3)

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where RichFunc3(c)=true"

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMultipleUserDefinedScalarFunctions(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where " +
      "RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2"

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello\nHello world"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
