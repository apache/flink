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
package org.apache.flink.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.plan.TreeNode
import org.apache.flink.table.validate.{ValidationResult, ValidationSuccess}

abstract class Expression extends TreeNode[Expression] {
  /**
    * Returns the [[TypeInformation]] for evaluating this expression.
    * It is sometimes not available until the expression is valid.
    */
  private[flink] def resultType: TypeInformation[_]

  /**
    * One pass validation of the expression tree in post order.
    */
  private[flink] lazy val valid: Boolean = childrenValid && validateInput().isSuccess

  private[flink] def childrenValid: Boolean = children.forall(_.valid)

  /**
    * Check input data types, inputs number or other properties specified by this expression.
    * Return `ValidationSuccess` if it pass the check,
    * or `ValidationFailure` with supplement message explaining the error.
    * Note: we should only call this method until `childrenValid == true`
    */
  private[flink] def validateInput(): ValidationResult = ValidationSuccess

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException(
      s"${this.getClass.getName} cannot be transformed to RexNode"
    )

  private[flink] def checkEquals(other: Expression): Boolean = {
    if (this.getClass != other.getClass) {
      false
    } else {
      def checkEquality(elements1: Seq[Any], elements2: Seq[Any]): Boolean = {
        elements1.length == elements2.length && elements1.zip(elements2).forall {
          case (e1: Expression, e2: Expression) => e1.checkEquals(e2)
          case (t1: Seq[_], t2: Seq[_]) => checkEquality(t1, t2)
          case (i1, i2) => i1 == i2
        }
      }
      val elements1 = this.productIterator.toSeq
      val elements2 = other.productIterator.toSeq
      checkEquality(elements1, elements2)
    }
  }
}

abstract class BinaryExpression extends Expression {
  private[flink] def left: Expression
  private[flink] def right: Expression
  private[flink] def children = Seq(left, right)
}

abstract class UnaryExpression extends Expression {
  private[flink] def child: Expression
  private[flink] def children = Seq(child)
}

abstract class LeafExpression extends Expression {
  private[flink] val children = Nil
}
