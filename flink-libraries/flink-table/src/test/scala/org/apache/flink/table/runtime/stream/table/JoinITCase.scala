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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamingWithStateTestBase}
import org.junit.Assert._
import org.junit.Test
import org.apache.flink.api.common.time.Time
import org.apache.flink.types.Row

class JoinITCase extends StreamingWithStateTestBase {

  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

  @Test
  def testOutputWithPk(): Unit = {
    // data input

    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5)
    )

    val data2 = List(
      (1, 1),
      (2, 0),
      (2, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5),
      (6, 6)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c)

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.max as 'b)

    val rightTableWithPk = rightTable
        .groupBy('bb)
        .select('bb, 'c.max as 'c)

    leftTableWithPk
      .join(rightTableWithPk, 'b === 'bb)
      .select('a, 'b, 'c)
      .writeToSink(new TestUpsertSink(Array("a,b"), false), queryConfig)

    env.execute()
    val results = RowCollector.getAndClearValues
    val retracted = RowCollector.upsertResults(results, Array(0)).sorted

    val expected = Seq("1,1,1", "2,2,2", "3,3,3", "4,4,4", "5,5,5")
    assertEquals(expected, retracted)

  }


  @Test
  def testOutputWithoutPk(): Unit = {
    // data input

    val data1 = List(
      (0, 0),
      (1, 0),
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 4),
      (5, 5)
    )

    val data2 = List(
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (1, 1, 1),
      (2, 2, 2),
      (3, 3, 3),
      (4, 4, 4),
      (5, 5, 5),
      (5, 5, 5),
      (6, 6, 6)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val leftTable = env.fromCollection(data1).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(data2).toTable(tEnv, 'bb, 'c, 'd)

    val leftTableWithPk = leftTable
      .groupBy('a)
      .select('a, 'b.max as 'b)

    leftTableWithPk
      .join(rightTable, 'a === 'bb && ('a < 4 || 'a > 4))
      .select('a, 'b, 'c, 'd)
      .writeToSink(new TestRetractSink, queryConfig)

    env.execute()
    val results = RowCollector.getAndClearValues

    val retracted = RowCollector.retractResults(results).sorted

    val expected = Seq("1,1,1,1", "1,1,1,1", "1,1,1,1", "1,1,1,1", "2,2,2,2", "3,3,3,3",
                       "5,5,5,5", "5,5,5,5")
    assertEquals(expected, retracted)
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testLeftOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val leftTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'bb, 'c)

    leftTable.leftOuterJoin(rightTable, 'a ==='bb).toAppendStream[Row]
    env.execute()
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testRightOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val leftTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'bb, 'c)

    leftTable.rightOuterJoin(rightTable, 'a ==='bb).toAppendStream[Row]
    env.execute()
  }

  @Test(expected = classOf[UnsupportedOperationException])
  def testFullOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)

    val leftTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'a, 'b)
    val rightTable = env.fromCollection(List((1, 2))).toTable(tEnv, 'bb, 'c)

    leftTable.fullOuterJoin(rightTable, 'a ==='bb).toAppendStream[Row]
    env.execute()
  }
}
