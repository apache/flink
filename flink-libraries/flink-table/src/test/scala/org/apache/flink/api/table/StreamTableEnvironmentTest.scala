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
package org.apache.flink.api.table

import org.apache.flink.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.api.scala.table._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Test
import org.junit.Assert._

import scala.collection.mutable


class StreamTableEnvironmentTest extends StreamingMultipleProgramsTestBase{

  @Test
  def testReduceExpression(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT STREAM (3+4)+a, b+(1+2), c, 5+6 FROM MyTable WHERE a>(1+7)"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val table = tEnv.sql(sqlQuery)

    val optimized = tEnv.optimize(table)
    assertTrue(optimized.toString.contains("+(7, _1)"))
    assertTrue(optimized.toString.contains("+(_2, 3)"))
    assertTrue(optimized.toString.contains(">(_1, 8)"))
  }

}
