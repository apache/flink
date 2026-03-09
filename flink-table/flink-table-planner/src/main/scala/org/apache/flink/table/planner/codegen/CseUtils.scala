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
package org.apache.flink.table.planner.codegen

import org.apache.calcite.rex.{RexCall, RexNode, RexVisitorImpl}

import scala.collection.mutable

/**
 * Utility for Common Sub-expression Elimination (CSE) analysis.
 *
 * <p>This utility scans a list of [[RexNode]] expressions and identifies sub-expressions
 * (specifically [[RexCall]] nodes) that appear more than once. These duplicates are candidates for
 * CSE: they should be evaluated once and the result cached in a local variable.
 *
 * <p>Only [[RexCall]] nodes are considered as CSE candidates because: <ul> <li>Input references and
 * literals are trivially cheap to evaluate.</li> <li>Function calls (especially UDFs) can be
 * expensive and benefit most from caching.</li> </ul>
 *
 * <p>Usage example: Given projections like
 * {{{
 *   testcse2(testcse(sid)), testcse3(testcse(sid)), testcse4(testcse(sid))
 * }}}
 * the analyzer will identify {@code testcse(sid)} as a common sub-expression since it appears 3
 * times. The code generator can then evaluate it once and reuse the result.
 */
object CseUtils {

  /**
   * Analyzes a list of RexNode expressions and returns the set of RexNode digest strings that
   * appear more than once (i.e., duplicate sub-expressions eligible for CSE).
   *
   * @param rexNodes
   *   the list of RexNode expressions to analyze (e.g., projection list)
   * @return
   *   a set of RexNode digest strings that are duplicated across the expressions
   */
  def findDuplicateSubExpressions(rexNodes: Seq[RexNode]): Set[String] = {
    val counter = mutable.Map[String, Int]()
    val visitor = new SubExpressionCounter(counter)
    rexNodes.foreach(_.accept(visitor))
    counter.filter(_._2 > 1).keySet.toSet
  }

  /**
   * A RexVisitor that counts occurrences of each [[RexCall]] sub-expression by its digest. The
   * visitor traverses the entire expression tree, counting every [[RexCall]] node (including deeply
   * nested ones).
   */
  private class SubExpressionCounter(counter: mutable.Map[String, Int])
    extends RexVisitorImpl[Unit](true) {

    override def visitCall(call: RexCall): Unit = {
      val digest = call.toString
      counter.put(digest, counter.getOrElse(digest, 0) + 1)
      // Continue visiting child operands
      super.visitCall(call)
    }
  }
}
