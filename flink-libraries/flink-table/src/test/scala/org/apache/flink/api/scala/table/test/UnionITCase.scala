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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{TableException, TableEnvironment, Row}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import TableProgramsTestBase.TableConfigMode

@RunWith(classOf[Parameterized])
class UnionITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testUnion(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTernaryUnion(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds3 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2).unionAll(ds3).select('c)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n" +
      "Hi\n" + "Hello\n" + "Hello world\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnionWithFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    if (tEnv.getConfig.getEfficientTypeUsage) {
      return
    }

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val joinDs = ds1.unionAll(ds2.select('a, 'b, 'c)).filter('b < 2).select('c)

    val results = joinDs.toDataSet[Row].collect()
    val expected = "Hi\n" + "Hallo\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionDifferentFieldNames(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    // must fail. Union inputs have different field names.
    ds1.unionAll(ds2)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnionDifferentFieldTypes(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    // must fail. Union inputs have different field types.
    ds1.unionAll(ds2)
  }

  @Test
  def testUnionWithAggregation(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    if (tEnv.getConfig.getEfficientTypeUsage) {
      return
    }

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2.select('a, 'b, 'c)).select('c.count)

    val results = unionDs.toDataSet[Row].collect()
    val expected = "18"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUnionWithJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    if (tEnv.getConfig.getEfficientTypeUsage) {
      return
    }

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv,'a, 'b, 'd, 'c, 'e)
    val ds3 = CollectionDataSets.getSmall5TupleDataSet(env).toTable(tEnv, 'a2, 'b2, 'd2, 'c2, 'e2)

    val joinDs = ds1.unionAll(ds2.select('a, 'b, 'c))
      .join(ds3.select('a2, 'b2, 'c2))
      .where('a ==='a2).select('c, 'c2)

    val results = joinDs.toDataSet[Row].collect()
    val expected = "Hi,Hallo\n" + "Hallo,Hallo\n" +
      "Hello,Hallo Welt\n" + "Hello,Hallo Welt wie\n" +
      "Hallo Welt,Hallo Welt\n" + "Hallo Welt wie,Hallo Welt\n" +
      "Hallo Welt,Hallo Welt wie\n" + "Hallo Welt wie,Hallo Welt wie\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[TableException])
  def testUnionTablesFromDifferentEnvs(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env, config)
    val tEnv2 = TableEnvironment.getTableEnvironment(env, config)

    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2).select('c)
  }
}
