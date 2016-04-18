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
package org.apache.flink.api.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.trees.TreeNode
import org.apache.flink.api.table.validate.ExprValidationResult

abstract class Expression extends TreeNode[Expression] {
  /**
    * Returns the [[TypeInformation]] for evaluating this expression.
    * It is sometimes available until the expression is valid.
    */
  def dataType: TypeInformation[_]

  /**
    * One pass validation of the expression tree in post order.
    */
  lazy val valid: Boolean = childrenValid && validateInput().isSuccess

  def childrenValid: Boolean = children.forall(_.valid)

  /**
    * Check input data types, inputs number or other properties specified by this expression.
    * Return `ValidationSuccess` if it pass the check, or `ValidationFailure` with supplement message
    * Note: we should only call this method until `childrenValidated == true`
    */
  def validateInput(): ExprValidationResult = ExprValidationResult.ValidationSuccess

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException(
      s"${this.getClass.getName} cannot be transformed to RexNode"
    )
}

abstract class BinaryExpression extends Expression {
  def left: Expression
  def right: Expression
  def children = Seq(left, right)
}

abstract class UnaryExpression extends Expression {
  def child: Expression
  def children = Seq(child)
}

abstract class LeafExpression extends Expression {
  val children = Nil
}
