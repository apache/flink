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

package org.apache.flink.table.planner.plan.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.planner.utils.TableTestUtil
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Test

class UnsupportedOpsValidationTest extends AbstractTestBase {

  @Test(expected = classOf[ValidationException])
  def testSort(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    env.fromCollection(TestData.smallTupleData3).toTable(tEnv).orderBy('_1.desc)
  }

  @Test(expected = classOf[ValidationException])
  def testJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.join(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.union(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testIntersect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.intersect(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testIntersectAll(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.intersectAll(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testMinus(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.minus(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testMinusAll(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    val t2 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.minusAll(t2)
  }

  @Test(expected = classOf[ValidationException])
  def testOffset(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.offset(5)
  }

  @Test(expected = classOf[ValidationException])
  def testFetch(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)
    val t1 = env.fromCollection(TestData.smallTupleData3).toTable(tEnv)
    t1.fetch(5)
  }

}
