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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class SetOperatorsValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testUnionFieldsNameNotOverlap1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'd, 'c, 'e)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test(expected = classOf[ValidationException])
  def testUnionFieldsNameNotOverlap2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .select('a, 'b, 'c)

    val unionDs = ds1.unionAll(ds2)

    val results = unionDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test(expected = classOf[ValidationException])
  def testUnionTablesFromDifferentEnv(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = StreamTableEnvironment.create(env)
    val tEnv2 = StreamTableEnvironment.create(env)

    val ds1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv1, 'a, 'b, 'c)
    val ds2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv2, 'a, 'b, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.unionAll(ds2)
  }
}
