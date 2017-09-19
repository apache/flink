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

package org.apache.flink.table.plan.optimize

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.rules.ReduceExpressionsRule
import org.apache.calcite.tools.RuleSets
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import org.mockito.Mockito._

/**
  * Tests for [[FlinkChainedPrograms]].
  */
class FlinkChainedProgramsTest {

  @Test
  def testAddGetRemovePrograms(): Unit = {
    val programs = new FlinkChainedPrograms
    assertTrue(programs.getProgramNames.isEmpty)
    assertTrue(programs.get("o1").isEmpty)

    val program1 = FlinkHepProgramBuilder.newBuilder
      .add(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build()
    var result = programs.addFirst("o2", program1)
    assertTrue(result)
    assertEquals(Seq("o2"), programs.getProgramNames.asScala)
    assertTrue(programs.get("o2").isDefined)
    assertTrue(program1 == programs.get("o2").get)

    val program2 = FlinkHepProgramBuilder.newBuilder
      .add(RuleSets.ofList(ReduceExpressionsRule.CALC_INSTANCE))
      .build()
    result = programs.addFirst("o1", program2)
    assertTrue(result)
    assertEquals(Seq("o1", "o2"), programs.getProgramNames.asScala)
    assertTrue(programs.get("o1").isDefined)
    assertTrue(program2 == programs.get("o1").get)

    val program3 = FlinkHepProgramBuilder.newBuilder
      .add(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE))
      .setMatchLimit(100)
      .build()
    result = programs.addLast("o4", program3)
    assertTrue(result)
    assertEquals(Seq("o1", "o2", "o4"), programs.getProgramNames.asScala)
    assertTrue(programs.get("o4").isDefined)
    assertTrue(program3 == programs.get("o4").get)

    val program4 = FlinkHepProgramBuilder.newBuilder
      .add(RuleSets.ofList(ReduceExpressionsRule.JOIN_INSTANCE))
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build()
    result = programs.addBefore("o4", "o3", program4)
    assertTrue(result)
    assertEquals(Seq("o1", "o2", "o3", "o4"), programs.getProgramNames.asScala)
    assertTrue(programs.get("o3").isDefined)
    assertTrue(program4 == programs.get("o3").get)

    var p = programs.remove("o2")
    assertTrue(p.isDefined)
    assertTrue(p.get == program1)
    p = programs.remove("o0")
    assertTrue(p.isEmpty)
    assertEquals(Seq("o1", "o3", "o4"), programs.getProgramNames.asScala)

    result = programs.addFirst("o3", program1)
    assertFalse(result)
    result = programs.addLast("o4", program1)
    assertFalse(result)
    result = programs.addBefore("o0", "o4", program1)
    assertFalse(result)
    assertEquals(Seq("o1", "o3", "o4"), programs.getProgramNames.asScala)
  }

  @Test
  def testGetFlinkRuleSetProgram(): Unit = {
    val programs = new FlinkChainedPrograms
    assertTrue(programs.getProgramNames.isEmpty)

    val program1 = FlinkHepProgramBuilder.newBuilder
      .add(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .build()
    programs.addFirst("o1", program1)
    assertTrue(programs.getFlinkRuleSetProgram("o1").isDefined)
    assertTrue(program1 == programs.getFlinkRuleSetProgram("o1").get)

    val program2 = new FlinkDecorrelateProgram
    programs.addLast("o2", program2)
    assertTrue(programs.get("o2").isDefined)
    assertTrue(program2 == programs.get("o2").get)
    assertTrue(programs.getFlinkRuleSetProgram("o2").isEmpty)

    assertTrue(programs.getFlinkRuleSetProgram("o3").isEmpty)
  }

  @Test(expected = classOf[RuntimeException])
  def testOptimizeWithNotExistProgram(): Unit = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]
    programs.addLast("o1", null)
    val scan = mock(classOf[TableScan])
    val context = mock(classOf[BatchOptimizeContext])
    programs.optimize(scan, context)
  }
}
