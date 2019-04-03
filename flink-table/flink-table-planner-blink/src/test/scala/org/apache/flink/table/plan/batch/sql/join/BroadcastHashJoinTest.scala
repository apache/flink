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

import org.apache.flink.table.api.{PlannerConfigOptions, TableConfigOptions, TableException}

import org.junit.{Before, Test}

class BroadcastHashJoinTest extends JoinTestBase {

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setLong(
      PlannerConfigOptions.SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD, Long.MaxValue)
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin, NestedLoopJoin, ShuffleHashJoin")
  }

  @Test
  override def testJoinNonMatchingKeyTypes(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Equality join predicate on incompatible types")
    super.testJoinNonMatchingKeyTypes()
  }

  @Test
  override def testInnerJoinWithoutJoinPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInnerJoinWithoutJoinPred()
  }

  @Test
  override def testInnerJoinWithNonEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testInnerJoinWithNonEquiPred()
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
  override def testFullOuterJoinWithEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinWithEquiPred()
  }

  @Test
  override def testFullOuterJoinWithEquiAndLocalPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinWithEquiAndLocalPred()
  }

  @Test
  override def testFullOuterJoinWithEquiAndNonEquiPred(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Cannot generate a valid execution plan for the given query")
    super.testFullOuterJoinWithEquiAndNonEquiPred()
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
}
