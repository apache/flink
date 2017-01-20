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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.UDFTestUtils
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.expressions.utils.{RichFunc0, RichFunc1, RichFunc2}
import org.apache.flink.types.Row
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

class UserDefinedScalarFunctionITCase extends StreamingMultipleProgramsTestBase {

  @Before
  def setup(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testOpenClose(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc0", RichFunc0)

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("t1", ds, 'a, 'b, 'c)

    val sqlQuery = "SELECT c FROM t1 where RichFunc0(a)=4"

    val result = tEnv.sql(sqlQuery)
    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello world")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testSingleUDFWithoutParameter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", RichFunc2)

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='#Hello'"

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testSingleUDFWithParameter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", RichFunc2)
    UDFTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val sqlQuery = "SELECT c FROM t1 where RichFunc2(c)='ABC#Hello'"

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    StreamITCase.compareWithList(expected.asJava)
  }

  @Test
  def testMultiUDFs(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc0", RichFunc0)
    tEnv.registerFunction("RichFunc1", RichFunc1)
    tEnv.registerFunction("RichFunc2", RichFunc2)
    UDFTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val sqlQuery = "SELECT c FROM t1 where " +
      "RichFunc0(a)=3 and RichFunc2(c)='Abc#Hello' or RichFunc1(a)=3 and b=2"

    val ds = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("t1", ds, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery)

    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    StreamITCase.compareWithList(expected.asJava)
  }

}
