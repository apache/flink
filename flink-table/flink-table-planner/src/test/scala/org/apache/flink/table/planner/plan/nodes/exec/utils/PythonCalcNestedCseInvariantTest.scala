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
package org.apache.flink.table.planner.plan.nodes.exec.utils

import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCalc
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexCall, RexNode}
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConverters._

/**
 * Tests the invariants of nested Python UDF flattening, driven through the real SQL pipeline so
 * that genuine Python [[RexCall]]s are used.
 *
 * <p>Flattening happens while translating the ExecNode into a transformation and is therefore not
 * visible in the optimized plan, so these invariants are asserted here rather than in a plan test.
 */
class PythonCalcNestedCseInvariantTest extends TableTestBase {

  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addTemporarySystemFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.addTemporarySystemFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))
  }

  /** Collects the single PythonCalc of the given query, together with its CSE result. */
  private def analyze(sql: String): (Int, Int, PythonCallCseResult, List[RexCall]) = {
    val table = util.tableEnv.sqlQuery(sql)
    val optimized =
      util.getPlanner.optimize(TableTestUtil.toRelNode(table)).asInstanceOf[StreamPhysicalRel]
    val graph = util.getPlanner
      .translateToExecNodeGraph(Seq(optimized).toArray[RelNode], isCompiled = false)

    var found: Option[(Int, Int, PythonCallCseResult, List[RexCall])] = None
    def visit(node: ExecNode[_]): Unit = {
      node match {
        case calc: CommonExecPythonCalc if found.isEmpty =>
          val f = classOf[CommonExecPythonCalc].getDeclaredField("projection")
          f.setAccessible(true)
          val projection = f.get(calc).asInstanceOf[java.util.List[RexNode]]
          val topCalls = projection.asScala.collect { case c: RexCall => c }.toList
          val forwarded = projection.asScala.count(!_.isInstanceOf[RexCall])
          val outWidth = calc.getOutputType.asInstanceOf[RowType].getFieldCount
          found = Some(
            (forwarded, outWidth, PythonCallDeduplicator.deduplicate(topCalls.asJava), topCalls))
        case _ =>
      }
      node.getInputEdges.asScala.foreach(e => visit(e.getSource))
    }
    graph.getRootNodes.asScala.foreach(visit)
    found.getOrElse(throw new AssertionError(s"no PythonCalc found for: $sql"))
  }

  /**
   * The worker evaluates the flattened list sequentially and reads earlier results by index, so a
   * referenced sub-expression must always be evaluated before the call referencing it.
   */
  private def assertReferencesResolveBackwards(res: PythonCallCseResult): Unit = {
    val flattened = res.getDeduplicatedCalls.asScala.toList
    val refMap = res.getRefMap.asScala
    flattened.zipWithIndex.foreach {
      case (call, idx) =>
        call.getOperands.asScala.foreach {
          case operand: RexCall =>
            refMap.get(operand).foreach {
              refIdx =>
                assertThat(refIdx)
                  .as(
                    s"flattened[$idx] references results[$refIdx], which must be computed earlier")
                  .isLessThan(idx)
            }
          case _ =>
        }
    }
  }

  /** The projected results must be exactly as wide as the operator's UDF output columns. */
  private def assertOutputWidthMatches(
      forwarded: Int,
      outWidth: Int,
      res: PythonCallCseResult,
      topCalls: List[RexCall]): Unit = {
    val flattened = res.getDeduplicatedCalls.asScala.toList
    val outputIndices = res.getOutputIndices

    assertThat(outputIndices.length)
      .as("one output index per projected call")
      .isEqualTo(topCalls.size)
    assertThat(outWidth - forwarded)
      .as("the operator output must have one column per projected call")
      .isEqualTo(outputIndices.length)
    outputIndices.zipWithIndex.foreach {
      case (flatIdx, projIdx) =>
        assertThat(flattened(flatIdx))
          .as(s"output index $projIdx must point at the projected call")
          .isEqualTo(topCalls(projIdx))
    }
  }

  private def check(sql: String, expectedFlattened: Int): Unit = {
    val (forwarded, outWidth, res, topCalls) = analyze(sql)
    assertThat(res.getDeduplicatedCalls.size())
      .as(s"number of evaluated calls for: $sql")
      .isEqualTo(expectedFlattened)
    assertReferencesResolveBackwards(res)
    assertOutputWidthMatches(forwarded, outWidth, res, topCalls)
  }

  @Test
  def testNestedCallIsFlattened(): Unit = {
    // the inner call becomes a separate evaluation referenced by the outer one
    check("SELECT pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2)
  }

  @Test
  def testSharedInnerCallIsEvaluatedOnce(): Unit = {
    // pyFunc1(a, b) is both projected and nested, but must be evaluated only once
    check("SELECT pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2)
  }

  @Test
  def testSharedInnerCallProjectedAfterOuter(): Unit = {
    // same as above but the outer call comes first, so the reference points backwards only if
    // post-order is preserved
    check("SELECT pyFunc2(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable", 2)
  }

  @Test
  def testTwoDistinctNestedCalls(): Unit = {
    check("SELECT pyFunc3(pyFunc1(a, b), pyFunc2(b, c)), c FROM MyTable", 3)
  }

  @Test
  def testDeeplyNestedCalls(): Unit = {
    check("SELECT pyFunc2(pyFunc2(pyFunc1(a, b), c), c) FROM MyTable", 3)
  }

  @Test
  def testForwardedFieldsAreNotAffected(): Unit = {
    check("SELECT c, pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2)
  }
}
