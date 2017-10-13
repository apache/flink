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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.runtime.utils.StreamITCase
import org.apache.flink.types.Row
import org.junit.Test

/**
  * Test if the table joins can be correctly translated.
  */
class JoinITCase extends StreamingMultipleProgramsTestBase {

  /**
    * Test for proctime window inner join.
    */
  @Test
  def testProctimeWindowJoin(): Unit = {
    val data = List()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    val leftStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val rightStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val leftTable = leftStream.toTable(tEnv, 'a, 'b, 'c, 'ltime.proctime)
    val rightTable = rightStream.toTable(tEnv, 'd, 'e, 'f, 'rtime.proctime)
    leftTable
      .join(rightTable)
      .where('a === 'd && 'b === 'e && 'ltime > 'rtime - 4.seconds && 'ltime <= 'rtime)
      .toAppendStream[Row]
    env.execute()
  }

  /**
    * Test for rowtime window inner join.
    */
  @Test
  def testRowtimeWindowJoin(): Unit = {
    val data = List()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val leftStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val rightStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val leftTable = leftStream.toTable(tEnv, 'a, 'b, 'c, 'ltime.rowtime)
    val rightTable = rightStream.toTable(tEnv, 'd, 'e, 'f, 'rtime.rowtime)
    leftTable
      .join(rightTable)
      .where('a === 'd && 'ltime > 'rtime - 5.seconds && 'ltime <= 'rtime + 1.second)
      .select('a, 'e, 'ltime) // Only one rowtime field can exist in the result stream.
      .toAppendStream[Row]
    env.execute()
  }

  /**
    * There must be complete window-bounds.
    */
  @Test(expected = classOf[TableException])
  def testIncompleteWindowBounds(): Unit = {
    val data = List()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val leftStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val rightStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val leftTable = leftStream.toTable(tEnv, 'a, 'b, 'c, 'ltime.proctime)
    val rightTable = rightStream.toTable(tEnv, 'd, 'e, 'f, 'rtime.proctime)
    leftTable
      .join(rightTable)
      .where('a === 'd && 'ltime > 'rtime - 5.seconds && 'ltime < 'ltime + 5.seconds)
      .toAppendStream[Row]
    env.execute()
  }

  /**
    * There must be at least one equi-join predicate.
    */
  @Test(expected = classOf[TableException])
  def testNoEuqiPredicate(): Unit = {
    val data = List()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val leftStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val rightStream: DataStream[(Long, Int, String)] = env.fromCollection(data)
    val leftTable = leftStream.toTable(tEnv, 'a, 'b, 'c, 'lTime.proctime)
    val rightTable = rightStream.toTable(tEnv, 'd, 'e, 'f, 'rTime.proctime)
    leftTable
      .join(rightTable)
      .where('lTime > 'rTime - 5.seconds && 'lTime < 'rTime + 5.seconds)
      .toAppendStream[Row]
    env.execute()
  }
}
