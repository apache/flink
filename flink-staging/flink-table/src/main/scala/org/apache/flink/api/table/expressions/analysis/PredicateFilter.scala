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

package org.apache.flink.api.table.expressions.analysis

import org.apache.flink.api.table.expressions._

object PredicateFilter {

  /**
   * Prunes an expression according to the given filter.
   * One part of a conjunction remains if the other part is filtered out.
   * A disjunction is removed if one part is filtered out.
   *
   * @param filter filter function
   * @param expr expression to be copied and pruned
   * @return new pruned expression
   */
  def pruneExpr(filter: (Expression) => Boolean, expr: Expression): Expression = {
    expr match {
      case And(left, right) =>
        val prunedLeft = pruneExpr(filter, left)
        val prunedRight = pruneExpr(filter, right)

        if (prunedLeft.isInstanceOf[NopExpression]) {
          prunedRight
        }
        else if (prunedRight.isInstanceOf[NopExpression]) {
          prunedLeft
        }
        else {
          And(prunedLeft, prunedRight)
        }
      case Or(left, right) =>
        if (filter(left) && filter(right)) {
          val prunedLeft = pruneExpr(filter, left)
          val prunedRight = pruneExpr(filter, right)

          if (prunedLeft.isInstanceOf[NopExpression] || prunedRight.isInstanceOf[NopExpression]) {
            NopExpression()
          }
          else {
            Or(prunedLeft, prunedRight)
          }
        }
        else {
          NopExpression()
        }
      case Not(e) =>
        if (filter(e)) {
          val prunedE = pruneExpr(filter, e)
          if (prunedE.isInstanceOf[NopExpression]){
            NopExpression()
          }
          else {
            Not(e)
          }
        }
        else {
          NopExpression()
        }
      case e: Expression =>
        if (filter(e)) {
          e
        }
        else {
          NopExpression()
        }
      case _ => NopExpression()
    }
  }

}
