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

class ShuffledHashJoinTest extends JoinTestBase {

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.SQL_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, BroadcastHashJoin")
  }

  @Test
  override def testInnerJoinWithoutJoinPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInnerJoinWithoutJoinPred()
  }

  @Test
  override def testLeftOuterJoinNoEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testLeftOuterJoinNoEquiPred()
  }

  @Test
  override def testLeftOuterJoinOnTrue(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testLeftOuterJoinOnTrue()
  }

  @Test
  override def testLeftOuterJoinOnFalse(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testLeftOuterJoinOnFalse()
  }

  @Test
  override def testRightOuterJoinOnTrue(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testRightOuterJoinOnTrue()
  }

  @Test
  override def testRightOuterJoinOnFalse(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testRightOuterJoinOnFalse()
  }

  @Test
  override def testRightOuterJoinWithNonEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testRightOuterJoinWithNonEquiPred()
  }

  @Test
  override def testFullOuterJoinWithNonEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinWithNonEquiPred()
  }

  @Test
  override def testFullOuterJoinOnFalse(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinOnFalse()
  }

  @Test
  override def testFullOuterJoinOnTrue(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinOnTrue()
  }

  @Test
  override def testCrossJoin(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testCrossJoin()
  }

  @Test
  override def testSelfJoin(): Unit = {
    // TODO use shuffle hash join if isBroadcast is true and isBroadcastHashJoinEnabled is false ?
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testSelfJoin()
  }
}
