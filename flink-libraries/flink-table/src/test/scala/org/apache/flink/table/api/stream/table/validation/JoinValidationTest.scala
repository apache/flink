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
import org.apache.flink.table.api.stream.table.validation.JoinValidationTest.WithoutEqualsHashCode
import org.apache.flink.table.api.{TableEnvironment, TableException, ValidationException}
import org.apache.flink.table.runtime.utils.StreamTestData
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Test

class JoinValidationTest extends TableTestBase {

  /**
    * Generic type cannot be used as key of map state.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidStateTypes(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tenv = TableEnvironment.getTableEnvironment(env)
    val ds = env.fromElements(new WithoutEqualsHashCode) // no equals/hashCode
    val t = tenv.fromDataStream(ds)

    val left = t.select('f0 as 'l)
    val right = t.select('f0 as 'r)

    val resultTable = left.join(right)
      .where('l === 'r)
      .select('l)

    resultTable.toRetractStream[Row]
  }

  /**
    * At least one equi-join predicate required.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithoutEquiPredicate(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 3.seconds)
      .select('a, 'e, 'ltime)

    val expected = ""
    util.verifyTable(resultTable, expected)
  }

  /**
    * At least one equi-join predicate required for non-window inner join.
    */
  @Test(expected = classOf[TableException])
  def testNonWindowInnerJoinWithoutEquiPredicate(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val resultTable = left.join(right)
      .select('a, 'e)

    val expected = ""
    util.verifyTable(resultTable, expected)
  }

  /**
    * There must be complete window-bounds.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithIncompleteWindowBounds1(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime < 'ltime + 3.seconds)
      .select('a, 'e, 'ltime)

    util.verifyTable(resultTable, "")
  }

  /**
    * There must be complete window-bounds.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithIncompleteWindowBounds2(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime > 'rtime + 3.seconds)
      .select('a, 'e, 'ltime)

    util.verifyTable(resultTable, "")
  }

  /**
    * Time indicators for the two tables must be identical.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithDifferentTimeIndicators(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.proctime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 3.seconds)

    util.verifyTable(resultTable, "")
  }

  @Test(expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g)
  }


  @Test(expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2.select('d, 'e, 'f, 'g, 'h as 'c))
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate1(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g)
      .toRetractStream[Row]
  }

  @Test(expected = classOf[TableException])
  def testNoEqualityJoinPredicate2(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g)
      .toRetractStream[Row]
  }

  @Test(expected = classOf[ValidationException])
  def testNoEquiJoin(): Unit = {
    val util = streamTestUtil()
    val ds1 = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.join(ds1, 'b < 'd).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)
    val ds1 = StreamTestData.get3TupleDataStream(env)
    val ds2 = StreamTestData.get5TupleDataStream(env)
    val in1 = tEnv1.fromDataStream(ds1, 'a, 'b, 'c)
    val in2 = tEnv2.fromDataStream(ds2, 'd, 'e, 'f, 'g, 'c)

    // Must fail. Tables are bound to different TableEnvironments.
    in1.join(in2).where('b === 'e).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvsJava() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)
    val ds1 = StreamTestData.get3TupleDataStream(env)
    val ds2 = StreamTestData.get5TupleDataStream(env)
    val in1 = tEnv1.fromDataStream(ds1, 'a, 'b, 'c)
    val in2 = tEnv2.fromDataStream(ds2, 'd, 'e, 'f, 'g, 'c)
    // Must fail. Tables are bound to different TableEnvironments.
    in1.join(in2).where("a === d").select("g.count")
  }
}

object JoinValidationTest {
  class WithoutEqualsHashCode
}
