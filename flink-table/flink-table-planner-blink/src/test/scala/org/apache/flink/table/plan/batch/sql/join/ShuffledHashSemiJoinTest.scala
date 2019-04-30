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

package org.apache.flink.table.plan.batch.sql.join

import org.apache.flink.table.api.{TableConfigOptions, TableException}

import org.junit.{Before, Test}

class ShuffledHashSemiJoinTest extends SemiJoinTestBase {

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, BroadcastHashJoin")
  }

  // the following test cases will throw exception because NestedLoopJoin is disabled.
  @Test
  override def testNotInWithCorrelated_NonEquiCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithCorrelated_NonEquiCondition1()
  }

  @Test
  override def testNotInWithCorrelated_NonEquiCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithCorrelated_NonEquiCondition2()
  }

  @Test
  override def testExistsWithUncorrelated_SimpleCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_SimpleCondition1()
  }

  @Test
  override def testExistsWithUncorrelated_SimpleCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_SimpleCondition2()
  }

  @Test
  override def testInNotInExistsNotExists(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInNotInExistsNotExists()
  }

  @Test
  override def testExistsWithUncorrelated_LateralTableInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_LateralTableInSubQuery()
  }

  @Test
  override def testInWithUncorrelated_ComplexCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithUncorrelated_ComplexCondition3()
  }

  @Test
  override def testNotExistsWithUncorrelated_ComplexCondition(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotExistsWithUncorrelated_ComplexCondition()
  }

  @Test
  override def testExistsWithUncorrelated_JoinInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_JoinInSubQuery()
  }

  @Test
  override def testMultiNotExistsWithUncorrelated(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMultiNotExistsWithUncorrelated()
  }

  @Test
  override def testExistsWithUncorrelated_ComplexCondition(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_ComplexCondition()
  }

  @Test
  override def testNotExistsWithCorrelated_NonEquiCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotExistsWithCorrelated_NonEquiCondition1()
  }

  @Test
  override def testNotExistsWithCorrelated_NonEquiCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotExistsWithCorrelated_NonEquiCondition2()
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_ComplexCondition1()
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_ComplexCondition2()
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_ComplexCondition3()
  }

  @Test
  override def testExistsWithCorrelated_NonEquiCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithCorrelated_NonEquiCondition1()
  }

  @Test
  override def testExistsWithCorrelated_NonEquiCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithCorrelated_NonEquiCondition2()
  }

  @Test
  override def testMultiExistsWithCorrelate1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMultiExistsWithCorrelate1()
  }

  @Test
  override def testMultiExistsWithCorrelate2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMultiExistsWithCorrelate2()
  }

  @Test
  override def testNotInWithUncorrelated_MultiFields(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_MultiFields()
  }

  @Test
  override def testMultiNotInWithCorrelated(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMultiNotInWithCorrelated()
  }

  @Test
  override def testNotExistsWithUncorrelated_SimpleCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotExistsWithUncorrelated_SimpleCondition1()
  }

  @Test
  override def testNotExistsWithUncorrelated_SimpleCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotExistsWithUncorrelated_SimpleCondition2()
  }

  @Test
  override def testMultiExistsWithUncorrelated(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testMultiExistsWithUncorrelated()
  }

  @Test
  override def testExistsAndNotExists(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsAndNotExists()
  }

  @Test
  override def testInWithCorrelated_ComplexCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithCorrelated_ComplexCondition3()
  }

  @Test
  override def testExistsWithUncorrelated_UnionInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithUncorrelated_UnionInSubQuery()
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_SimpleCondition1()
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition2(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_SimpleCondition2()
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testNotInWithUncorrelated_SimpleCondition3()
  }

  @Test
  override def testExistsWithCorrelated_LateralTableInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testExistsWithCorrelated_LateralTableInSubQuery()
  }

  @Test
  override def testInWithUncorrelated_LateralTableInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithUncorrelated_LateralTableInSubQuery()
  }

  @Test
  override def testInWithCorrelated_LateralTableInSubQuery(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithCorrelated_LateralTableInSubQuery()
  }

}
