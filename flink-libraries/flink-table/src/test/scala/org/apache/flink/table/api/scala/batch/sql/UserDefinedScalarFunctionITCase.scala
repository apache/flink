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

package org.apache.flink.table.api.scala.batch.sql

import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.UDFTestUtils
import org.apache.flink.table.expressions.utils._
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test

import scala.collection.JavaConverters._

class UserDefinedScalarFunctionITCase {

  @Test
  def testOpenClose(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc0", RichFunc0)

    val sqlQuery = "SELECT c FROM t1 where RichFunc0(a)=4"

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello world"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSingleUDFWithoutParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", RichFunc2)

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='#Hello'"

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSingleUDFWithParameter(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", RichFunc2)
    UDFTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='ABC#Hello'"

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSingleUDFDistributedCache(): Unit = {
    val words = "Hello\nWord"
    val filePath = UDFTestUtils.writeCacheFile("test_words", words)
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile(filePath, "words")
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc3", RichFunc3)

    val sqlQuery = "SELECT c FROM t1 where RichFunc3(c)=true"

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testMultiUDFs(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc0", RichFunc0)
    tEnv.registerFunction("RichFunc1", RichFunc1)
    tEnv.registerFunction("RichFunc2", RichFunc2)
    UDFTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val sqlQuery = "SELECT c FROM t1 where " +
      "RichFunc0(a)=3 and RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2"

    val ds = CollectionDataSets.getSmall3TupleDataSet(env)
    tEnv.registerDataSet("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val expected = "Hello\nHello world"
    val results = result.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
