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

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.{BeforeEach, Test}

class ShuffledHashSemiAntiJoinTest extends SemiAntiJoinTestBase {

  @BeforeEach
  def before(): Unit = {
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, BroadcastHashJoin")
    // the result plan may contains NestedLoopJoin (singleRowJoin)
    // which is converted by BatchExecSingleRowJoinRule
  }

  // the following test cases will throw exception
  // because NestedLoopJoin(non-singleRowJoin) is disabled.
  @Test
  override def testNotInWithCorrelated_NonEquiCondition1(): Unit = {
    assertThatThrownBy(() => super.testNotInWithCorrelated_NonEquiCondition1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithCorrelated_NonEquiCondition2(): Unit = {
    assertThatThrownBy(() => super.testNotInWithCorrelated_NonEquiCondition2())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testInNotInExistsNotExists(): Unit = {
    assertThatThrownBy(() => super.testInNotInExistsNotExists())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testInWithUncorrelated_ComplexCondition3(): Unit = {
    assertThatThrownBy(() => super.testInWithUncorrelated_ComplexCondition3())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotExistsWithCorrelated_NonEquiCondition1(): Unit = {
    assertThatThrownBy(() => super.testNotExistsWithCorrelated_NonEquiCondition1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotExistsWithCorrelated_NonEquiCondition2(): Unit = {
    assertThatThrownBy(() => super.testNotExistsWithCorrelated_NonEquiCondition2())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition1(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_ComplexCondition1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition2(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_ComplexCondition2())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_ComplexCondition3(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_ComplexCondition3())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testExistsWithCorrelated_NonEquiCondition1(): Unit = {
    assertThatThrownBy(() => super.testExistsWithCorrelated_NonEquiCondition1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testExistsWithCorrelated_NonEquiCondition2(): Unit = {
    assertThatThrownBy(() => super.testExistsWithCorrelated_NonEquiCondition2())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testMultiExistsWithCorrelate1(): Unit = {
    assertThatThrownBy(() => super.testMultiExistsWithCorrelate1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_MultiFields(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_MultiFields())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testMultiNotInWithCorrelated(): Unit = {
    assertThatThrownBy(() => super.testMultiNotInWithCorrelated())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testInWithCorrelated_ComplexCondition3(): Unit = {
    assertThatThrownBy(() => super.testInWithCorrelated_ComplexCondition3())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition1(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_SimpleCondition1())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition2(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_SimpleCondition2())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testNotInWithUncorrelated_SimpleCondition3(): Unit = {
    assertThatThrownBy(() => super.testNotInWithUncorrelated_SimpleCondition3())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

}
