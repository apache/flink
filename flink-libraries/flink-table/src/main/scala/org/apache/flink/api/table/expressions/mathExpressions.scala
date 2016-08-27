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
import org.apache.flink.api.table.validate._

case class Abs(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ExprValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Abs")

  override def toString: String = s"abs($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ABS, child.toRexNode)
  }
}

case class Ceil(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Ceil")

  override def toString: String = s"ceil($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CEIL, child.toRexNode)
  }
}

case class Exp(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"exp($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.EXP, child.toRexNode)
  }
}


case class Floor(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ExprValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Floor")

  override def toString: String = s"floor($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.FLOOR, child.toRexNode)
  }
}

case class Log10(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"log10($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOG10, child.toRexNode)
  }
}

case class Ln(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"ln($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LN, child.toRexNode)
  }
}

case class Power(left: Expression, right: Expression) extends BinaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    DOUBLE_TYPE_INFO :: DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"pow($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, left.toRexNode, right.toRexNode)
  }
}
