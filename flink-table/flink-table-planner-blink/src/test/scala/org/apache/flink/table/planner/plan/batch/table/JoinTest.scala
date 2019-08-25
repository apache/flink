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

package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.batch.table.JoinTest.Merger
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Ignore, Test}

class JoinTest extends TableTestBase {

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z && 'b < 2).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.leftOuterJoin(s, 'a === 'z && 'b < 'x).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z && 'x < 2).select('b, 'x)

    util.verifyPlan(joined)
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.rightOuterJoin(s, 'a === 'z && 'b < 'x).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testFullOuterJoinEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.fullOuterJoin(s, 'a === 'z).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testFullOuterJoinEquiAndLocalPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.fullOuterJoin(s, 'a === 'z && 'b < 2).select('b, 'y)

    util.verifyPlan(joined)
  }

  @Test
  def testFullOuterJoinEquiAndNonEquiPred(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("T", 'a, 'b, 'c)
    val s = util.addTableSource[(Long, String, Int)]("S", 'x, 'y, 'z)

    val joined = t.fullOuterJoin(s, 'a === 'z && 'b < 'x).select('b, 'y)

    util.verifyPlan(joined)
  }

  // TODO [FLINK-7942] [table] Reduce aliasing in RexNodes
 // @Ignore
  @Test
  def testFilterJoinRule(): Unit = {
    val util = batchTestUtil()
    val t1 = util.addTableSource[(String, Int, Int)]('a, 'b, 'c)
    val t2 = util.addTableSource[(String, Int, Int)]('d, 'e, 'f)
    val results = t1
      .leftOuterJoin(t2, 'b === 'e)
      .select('c, Merger('c, 'f) as 'c0)
      .select(Merger('c, 'c0) as 'c1)
      .where('c1 >= 0)

    util.verifyPlan(results)
  }

  // TODO
  @Ignore("Non-equi-join could be supported later.")
  @Test
  def testFullJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    util.verifyPlan(ds2.fullOuterJoin(ds1, 'b < 'd).select('c, 'g))
  }

  // TODO
  @Ignore("Non-equi-join could be supported later.")
  @Test
  def testLeftJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    util.verifyPlan(ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g))
  }

  // TODO
  @Ignore("Non-equi-join could be supported later.")
  @Test
  def testRightJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    util.verifyPlan(ds2.rightOuterJoin(ds1, 'b < 'd).select('c, 'g))
  }

  @Test
  def testNoEqualityJoinPredicate1(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    util.verifyPlan(ds1.join(ds2)
      // must fail. No equality join predicate
      .where('d === 'f)
      .select('c, 'g))
  }

  @Test
  def testNoEqualityJoinPredicate2(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    util.verifyPlan(ds1.join(ds2)
      // must fail. No equality join predicate
      .where('a < 'd)
      .select('c, 'g))
  }
}

object JoinTest {

  @SerialVersionUID(1L)
  object Merger extends ScalarFunction {
    def eval(f0: Int, f1: Int): Int = {
      f0 + f1
    }
  }
}
