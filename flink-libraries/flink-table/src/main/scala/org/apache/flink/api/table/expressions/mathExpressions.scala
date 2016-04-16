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
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.typeutils.TypeCheckUtils
import org.apache.flink.api.table.validate.ExprValidationResult

case class Abs(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = child.dataType

  override def validateInput(): ExprValidationResult =
    TypeCheckUtils.assertNumericExpr(child.dataType, "Abs")

  override def toString(): String = s"abs($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ABS, child.toRexNode)
  }
}

case class Exp(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = DOUBLE_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (child.dataType == DOUBLE_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"exp only accept Double input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"exp($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.EXP, child.toRexNode)
  }
}

case class Log10(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = DOUBLE_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (child.dataType == DOUBLE_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"log10 only accept Double input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"log10($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOG10, child.toRexNode)
  }
}

case class Ln(child: Expression) extends UnaryExpression {
  override def dataType: TypeInformation[_] = DOUBLE_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (child.dataType == DOUBLE_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"ln only accept Double input, get ${child.dataType}")
    }
  }

  override def toString(): String = s"ln($child)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LN, child.toRexNode)
  }
}

case class Power(left: Expression, right: Expression) extends BinaryExpression {
  override def dataType: TypeInformation[_] = DOUBLE_TYPE_INFO

  // TODO: this could be loosened by enabling implicit cast
  override def validateInput(): ExprValidationResult = {
    if (left.dataType == DOUBLE_TYPE_INFO && right.dataType == DOUBLE_TYPE_INFO) {
      ExprValidationResult.ValidationSuccess
    } else {
      ExprValidationResult.ValidationFailure(
        s"power only accept Double input, get ${left.dataType} and ${right.dataType}")
    }
  }

  override def toString(): String = s"pow($left, $right)"

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, left.toRexNode, right.toRexNode)
  }
}
