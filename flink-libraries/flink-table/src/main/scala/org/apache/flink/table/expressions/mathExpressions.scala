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
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.validate._

import scala.collection.JavaConversions._

case class Abs(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Abs")

  override def toString: String = s"abs($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ABS, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Ceil(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.LONG

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Ceil")

  override def toString: String = s"ceil($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CEIL, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Exp(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] = DataTypes.DOUBLE :: Nil

  override def toString: String = s"exp($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.EXP, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}


case class Floor(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.LONG

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Floor")

  override def toString: String = s"floor($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.FLOOR, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Log10(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] = DataTypes.DOUBLE :: Nil

  override def toString: String = s"log10($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOG10, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Log2(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def expectedTypes: Seq[InternalType] = DataTypes.DOUBLE :: Nil

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.LOG2, child.toRexNode)
  }

  override def toString: String = s"log2($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Log(base: Expression, antilogarithm: Expression) extends Expression with InputTypeSpec {
  def this(antilogarithm: Expression) = this(null, antilogarithm)

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def children: Seq[Expression] =
    if (base == null) Seq(antilogarithm) else Seq(base, antilogarithm)

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq.fill(children.length)(DataTypes.DOUBLE)

  override def toString: String = s"log(${children.mkString(",")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    relBuilder.call(ScalarSqlFunctions.LOG, children.map(_.toRexNode))

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

object Log {
  def apply(antilogarithm: Expression): Log = Log(null, antilogarithm)
}

case class Ln(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] = DataTypes.DOUBLE :: Nil

  override def toString: String = s"ln($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Power(left: Expression, right: Expression) extends BinaryExpression with InputTypeSpec {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] =
    DataTypes.DOUBLE :: DataTypes.DOUBLE :: Nil

  override def toString: String = s"pow($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, left.toRexNode, right.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sinh(child: Expression) extends UnaryExpression {

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Sinh")

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.SINH, child.toRexNode)
  }

  override def toString = s"sinh($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sqrt(child: Expression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] =
    Seq(DataTypes.DOUBLE)

  override def toString: String = s"sqrt($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, child.toRexNode, Literal(0.5).toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sin(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Sin")

  override def toString: String = s"sin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Cos(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cos")

  override def toString: String = s"cos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.COS, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Tan(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Tan")

  override def toString: String = s"tan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.TAN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Tanh(child: Expression) extends UnaryExpression {

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.TANH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Tanh")

  override def toString = s"tanh($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Cot(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cot")

  override def toString: String = s"cot($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.COT, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Asin(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Asin")

  override def toString: String = s"asin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ASIN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Acos(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Acos")

  override def toString: String = s"acos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ACOS, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Atan(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Atan")

  override def toString: String = s"atan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ATAN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Atan2(y: Expression, x: Expression) extends BinaryExpression {

  override private[flink] def left = y

  override private[flink] def right = x

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput() = {
    TypeCheckUtils.assertNumericExpr(y.resultType, "atan2")
    TypeCheckUtils.assertNumericExpr(x.resultType, "atan2")
  }

  override def toString: String = s"atan2($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ATAN2, left.toRexNode, right.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Degrees(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Degrees")

  override def toString: String = s"degrees($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.DEGREES, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Radians(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Radians")

  override def toString: String = s"radians($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RADIANS, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sign(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sign")

  override def toString: String = s"sign($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIGN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Round(left: Expression, right: Expression) extends BinaryExpression {
  override private[flink] def resultType: InternalType = left.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isInteger(right.resultType)) {
      ValidationFailure(s"round right requires int, get " +
        s"$right : ${right.resultType}")
    }
    TypeCheckUtils.assertNumericExpr(left.resultType, s"round left :$left")
  }

  override def toString: String = s"round($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.ROUND, left.toRexNode, right.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Pi() extends LeafExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override def toString: String = s"pi()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.PI)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class E() extends LeafExpression {
  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override def toString: String = s"e()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.E)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Rand(seed: Expression) extends Expression with InputTypeSpec {

  def this() = this(null)

  override private[flink] def children: Seq[Expression] = if (seed != null) {
    seed :: Nil
  } else {
    Nil
  }

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def expectedTypes: Seq[InternalType] = if (seed != null) {
    DataTypes.INT :: Nil
  } else {
    Nil
  }

  override def toString: String = if (seed != null) {
    s"rand($seed)"
  } else {
    s"rand()"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class RandInteger(seed: Expression, bound: Expression) extends Expression with InputTypeSpec {

  def this(bound: Expression) = this(null, bound)

  override private[flink] def children: Seq[Expression] = if (seed != null) {
    seed :: bound :: Nil
  } else {
    bound :: Nil
  }

  override private[flink] def resultType: InternalType = DataTypes.INT

  override private[flink] def expectedTypes: Seq[InternalType] = if (seed != null) {
    DataTypes.INT :: DataTypes.INT :: Nil
  } else {
    DataTypes.INT :: Nil
  }

  override def toString: String = if (seed != null) {
    s"randInteger($seed, $bound)"
  } else {
    s"randInteger($bound)"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND_INTEGER, children.map(_.toRexNode))
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Bin(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertIntegerFamilyExpr(child.resultType, "Bin")

  override def toString: String = s"bin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.BIN, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Hex(child: Expression) extends UnaryExpression {
  override private[flink] def resultType: InternalType = DataTypes.STRING

  override private[flink] def validateInput(): ValidationResult = {
    if (TypeCheckUtils.isIntegerFamily(child.resultType) ||
        TypeCheckUtils.isString(child.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"hex() requires an integer or string input but was '${child.resultType}'.")
    }
  }

  override def toString: String = s"hex($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.HEX, child.toRexNode)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]) =
    logicalExprVisitor.visit(this)
}

case class Cosh(child: Expression) extends UnaryExpression {

  override private[flink] def resultType: InternalType = DataTypes.DOUBLE

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.COSH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cosh")

  override def toString = s"cosh($child)"

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
