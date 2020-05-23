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

package org.apache.flink.table.planner.plan.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.runtime.utils.CollectionBatchExecTable
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit._

class JoinValidationTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testJoinNonExistingKey(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'foo does not exist
      .where('foo === 'e)
      .select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithNonMatchingKeyTypes(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds1.join(ds2)
      // must fail. Field 'a is Int, and 'g is String
      .where('a === 'g)
      .select('c, 'g)

  }

  @Test(expected = classOf[ValidationException])
  def testJoinWithAmbiguousFields(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'c)

    ds1.join(ds2)
      // must fail. Both inputs share the same field 'c
      .where('a === 'd)
      .select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testLeftJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.leftOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testRightJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.rightOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testFullJoinNoEquiJoinPredicate(): Unit = {
    val util = batchTestUtil()
    val ds1 = util.addTableSource[(Int, Long, String)]("Table3",'a, 'b, 'c)
    val ds2 = util.addTableSource[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    ds2.fullOuterJoin(ds1, 'b < 'd).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvs(): Unit = {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tEnv1 = TableEnvironmentImpl.create(settings)
    val tEnv2 = TableEnvironmentImpl.create(settings)
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv1, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv2, "d, e, f, g, c")

    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where('b === 'e).select('c, 'g)
  }

  @Test(expected = classOf[ValidationException])
  def testJoinTablesFromDifferentEnvsJava() {
    val settings = EnvironmentSettings.newInstance().inBatchMode().build()
    val tEnv1 = TableEnvironmentImpl.create(settings)
    val tEnv2 = TableEnvironmentImpl.create(settings)
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tEnv1, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tEnv2, "d, e, f, g, c")
    // Must fail. Tables are bound to different TableEnvironments.
    ds1.join(ds2).where($"a" === $"d").select($"g".count)
  }

  @Test(expected = classOf[TableException])
  def testOuterJoinWithPythonFunctionInCondition(): Unit = {
    val util = batchTestUtil()
    val left = util.addTableSource[(Int, Long, String)]("left",'a, 'b, 'c)
    val right = util.addTableSource[(Int, Long, String)]("right",'d, 'e, 'f)
    val pyFunc = new PythonScalarFunction("pyFunc")
    val result = left.leftOuterJoin(right, 'a === 'd && pyFunc('a, 'd) === 'a + 'd)
    util.verifyPlan(result)
  }
}
