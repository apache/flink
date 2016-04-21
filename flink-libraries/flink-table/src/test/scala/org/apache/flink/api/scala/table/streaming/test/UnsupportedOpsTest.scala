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

package org.apache.flink.api.scala.table.streaming.test

import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.table.streaming.test.utils.{StreamITCase, StreamTestData}
import org.apache.flink.api.table.{TableException, TableEnvironment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test

import scala.collection.mutable

class UnsupportedOpsTest extends StreamingMultipleProgramsTestBase {

  @Test(expected = classOf[TableException])
  def testSelectWithAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1.min)
  }

  @Test(expected = classOf[TableException])
  def testGroupBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
      .groupBy('_1)
  }

  @Test(expected = classOf[TableException])
  def testDistinct(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).distinct()
  }

  @Test(expected = classOf[TableException])
  def testJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
    t1.join(t2)
  }
}
