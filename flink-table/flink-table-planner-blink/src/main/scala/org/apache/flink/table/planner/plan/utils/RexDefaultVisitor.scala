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

package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rex._

/**
  * Implementation of [[RexVisitor]] that redirects all calls into generic
  * [[RexDefaultVisitor#visitNode(org.apache.calcite.rex.RexNode)]] method.
  */
abstract class RexDefaultVisitor[R] extends RexVisitor[R] {

  override def visitFieldAccess(fieldAccess: RexFieldAccess): R =
    visitNode(fieldAccess)

  override def visitCall(call: RexCall): R =
    visitNode(call)

  override def visitInputRef(inputRef: RexInputRef): R =
    visitNode(inputRef)

  override def visitOver(over: RexOver): R =
    visitNode(over)

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): R =
    visitNode(correlVariable)

  override def visitLocalRef(localRef: RexLocalRef): R =
    visitNode(localRef)

  override def visitDynamicParam(dynamicParam: RexDynamicParam): R =
    visitNode(dynamicParam)

  override def visitRangeRef(rangeRef: RexRangeRef): R =
    visitNode(rangeRef)

  override def visitTableInputRef(tableRef: RexTableInputRef): R =
    visitNode(tableRef)

  override def visitPatternFieldRef(patternFieldRef: RexPatternFieldRef): R =
    visitNode(patternFieldRef)

  override def visitSubQuery(subQuery: RexSubQuery): R =
    visitNode(subQuery)

  override def visitLiteral(literal: RexLiteral): R =
    visitNode(literal)

  def visitNode(rexNode: RexNode): R
}
