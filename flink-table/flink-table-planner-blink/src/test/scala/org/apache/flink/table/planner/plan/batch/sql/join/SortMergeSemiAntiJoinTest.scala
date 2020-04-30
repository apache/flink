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

package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions

import org.junit.{Before, Test}

class SortMergeSemiAntiJoinTest extends SemiAntiJoinTestBase {

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    // the result plan may contains NestedLoopJoin (singleRowJoin)
    // which is converted by BatchExecSingleRowJoinRule
  }

  // the following test cases will throw exception
  // because NestedLoopJoin(non-singleRowJoin) is disabled.
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
  override def testInNotInExistsNotExists(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInNotInExistsNotExists()
  }

  @Test
  override def testInWithUncorrelated_ComplexCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithUncorrelated_ComplexCondition3()
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
  override def testInWithCorrelated_ComplexCondition3(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInWithCorrelated_ComplexCondition3()
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
}
