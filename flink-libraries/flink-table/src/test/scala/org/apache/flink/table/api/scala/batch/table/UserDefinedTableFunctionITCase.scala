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

package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala.batch.utils.{TableProgramsCollectionTestBase, UDFTestUtils}
import org.apache.flink.table.utils.{RichTableFunc0, RichTableFunc1}
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class UserDefinedTableFunctionITCase(
  configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testOpenClose(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc = new RichTableFunc0
    tEnv.registerFunction("RichTableFunc0", tableFunc)

    val result = CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc('c) as 's)
      .select('a, 's)

    val expected =
      "1,Hi\n2,Hello\n3,Hello world\n4,Hello world, how are you?\n5,I am fine.\n6,Luke Skywalker"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSingleUDTFWithParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc1", tableFunc)
    UDFTestUtils.setJobParameters(env, Map("word_separator" -> " "))

    val sqlQuery = "SELECT a, s FROM t1, LATERAL TABLE(RichTableFunc1(c)) as T(s)"

    val result = CollectionDataSets.getSmall3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc('c) as 's)
      .select('a, 's)

    val expected = "3,Hello\n3,world"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMultiUDTFs(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val tableFunc0 = new RichTableFunc0
    val tableFunc1 = new RichTableFunc1
    tEnv.registerFunction("RichTableFunc0", tableFunc0)
    tEnv.registerFunction("RichTableFunc1", tableFunc1)
    UDFTestUtils.setJobParameters(env, Map("word_separator" -> " "))

    val result = CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .join(tableFunc0('c) as 's)
      .join(tableFunc1('c) as 'x)
      .select('a, 's, 'x)

    val expected = "3,Hello world,Hello\n" +
      "3,Hello world,world\n" +
      "4,Hello world, how are you?,Hello\n" +
      "4,Hello world, how are you?,are\n" +
      "4,Hello world, how are you?,how\n" +
      "4,Hello world, how are you?,world,\n" +
      "4,Hello world, how are you?,you?\n" +
      "5,I am fine.,I\n" +
      "5,I am fine.,am\n" +
      "5,I am fine.,fine.\n" +
      "6,Luke Skywalker,Luke\n" +
      "6,Luke Skywalker,Skywalker"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
