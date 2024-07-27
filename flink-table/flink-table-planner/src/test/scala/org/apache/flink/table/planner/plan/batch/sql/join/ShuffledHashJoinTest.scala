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
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.{BeforeEach, Test}

class ShuffledHashJoinTest extends JoinTestBase {

  @BeforeEach
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(1L))
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, BroadcastHashJoin")
  }

  @Test
  override def testInnerJoinWithoutJoinPred(): Unit = {
    assertThatThrownBy(() => super.testInnerJoinWithoutJoinPred())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testLeftOuterJoinNoEquiPred(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinNoEquiPred())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testLeftOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinOnTrue())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testLeftOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testLeftOuterJoinOnFalse())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testRightOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinOnTrue())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testRightOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinOnFalse())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testRightOuterJoinWithNonEquiPred(): Unit = {
    assertThatThrownBy(() => super.testRightOuterJoinWithNonEquiPred())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testFullOuterJoinWithNonEquiPred(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinWithNonEquiPred())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testFullOuterJoinOnFalse(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinOnFalse())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testFullOuterJoinOnTrue(): Unit = {
    assertThatThrownBy(() => super.testFullOuterJoinOnTrue())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @Test
  override def testCrossJoin(): Unit = {
    assertThatThrownBy(() => super.testCrossJoin())
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }
}
