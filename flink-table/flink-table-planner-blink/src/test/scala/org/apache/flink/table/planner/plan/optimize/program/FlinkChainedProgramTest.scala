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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.hep.{HepMatchOrder, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.Assert._
import org.junit.Test

import java.util.Collections

import scala.collection.JavaConversions._

/**
  * Tests for [[FlinkChainedProgram]].
  */
class FlinkChainedProgramTest {

  @Test
  def testAddGetRemovePrograms(): Unit = {
    val programs = new FlinkChainedProgram
    assertTrue(programs.getProgramNames.isEmpty)
    assertTrue(programs.get("o1").isEmpty)

    // test addFirst
    val builder = new HepProgramBuilder()
    builder
      .addMatchLimit(10)
      .addMatchOrder(HepMatchOrder.ARBITRARY)
      .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
      .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
      .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
      .addMatchLimit(100)
      .addMatchOrder(HepMatchOrder.BOTTOM_UP)
      .addRuleCollection(Collections.singletonList(CoreRules.FILTER_VALUES_MERGE))
    val program1 = FlinkHepProgram(builder.build())
    assertTrue(programs.addFirst("o2", program1))
    assertEquals(List("o2"), programs.getProgramNames.toList)
    assertTrue(programs.get("o2").isDefined)
    assertTrue(program1 eq programs.get("o2").get)

    val program2 = FlinkHepRuleSetProgramBuilder.newBuilder
      .add(RuleSets.ofList(
        CoreRules.FILTER_REDUCE_EXPRESSIONS,
        CoreRules.PROJECT_REDUCE_EXPRESSIONS,
        CoreRules.CALC_REDUCE_EXPRESSIONS,
        CoreRules.JOIN_REDUCE_EXPRESSIONS
      )).build()
    assertTrue(programs.addFirst("o1", program2))
    assertEquals(List("o1", "o2"), programs.getProgramNames.toList)
    assertTrue(programs.get("o1").isDefined)
    assertTrue(program2 eq programs.get("o1").get)

    // test addLast
    val program3 = FlinkHepRuleSetProgramBuilder.newBuilder
      .add(RuleSets.ofList(
        CoreRules.FILTER_CALC_MERGE,
        CoreRules.PROJECT_CALC_MERGE,
        CoreRules.FILTER_TO_CALC,
        CoreRules.PROJECT_TO_CALC,
        CoreRules.CALC_MERGE))
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
      .setMatchLimit(10000)
      .setHepMatchOrder(HepMatchOrder.ARBITRARY)
      .build()
    assertTrue(programs.addLast("o4", program3))
    assertEquals(List("o1", "o2", "o4"), programs.getProgramNames.toList)
    assertTrue(programs.get("o4").isDefined)
    assertTrue(program3 eq programs.get("o4").get)

    // test addBefore
    val TEST = new Convention.Impl("TEST", classOf[RelNode])
    val program4 = FlinkVolcanoProgramBuilder.newBuilder
      .add(RuleSets.ofList(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH))
      .setRequiredOutputTraits(Array(TEST))
      .build()
    assertTrue(programs.addBefore("o4", "o3", program4))
    assertEquals(List("o1", "o2", "o3", "o4"), programs.getProgramNames.toList)
    assertTrue(programs.get("o3").isDefined)
    assertTrue(program4 eq programs.get("o3").get)

    // test remove
    val p2 = programs.remove("o2")
    assertTrue(p2.isDefined)
    assertTrue(p2.get eq program1)
    assertEquals(List("o1", "o3", "o4"), programs.getProgramNames.toList)
    assertTrue(programs.remove("o0").isEmpty)
    assertEquals(List("o1", "o3", "o4"), programs.getProgramNames.toList)

    // program already exists
    assertFalse(programs.addFirst("o3", program1))
    assertFalse(programs.addLast("o4", program1))
    assertFalse(programs.addBefore("o0", "o4", program1))
    assertEquals(List("o1", "o3", "o4"), programs.getProgramNames.toList)
  }

  @Test
  def testGetFlinkRuleSetProgram(): Unit = {
    val programs = new FlinkChainedProgram
    assertTrue(programs.getProgramNames.isEmpty)

    val program1 = FlinkHepRuleSetProgramBuilder.newBuilder
      .add(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build()
    programs.addFirst("o1", program1)
    assertTrue(programs.get("o1").isDefined)
    assertTrue(program1 eq programs.get("o1").get)

    val builder = new HepProgramBuilder()
    builder
      .addMatchLimit(10)
      .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
      .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
      .addMatchOrder(HepMatchOrder.BOTTOM_UP)
    val program2 = FlinkHepProgram(builder.build())
    programs.addLast("o2", program2)
    assertTrue(programs.get("o2").isDefined)
    assertTrue(program2 eq programs.get("o2").get)
    assertTrue(programs.getFlinkRuleSetProgram("o2").isEmpty)

    assertTrue(programs.getFlinkRuleSetProgram("o3").isEmpty)

    val p1 = programs.getFlinkRuleSetProgram("o1")
    assertTrue(p1.isDefined)
    p1.get.add(RuleSets.ofList(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE))
    assertTrue(p1.get eq programs.getFlinkRuleSetProgram("o1").get)
  }

  @Test(expected = classOf[NullPointerException])
  def testAddNullProgram(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]
    programs.addLast("o1", null)
  }
}
