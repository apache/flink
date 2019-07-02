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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.typeutils.TypeInfoCheckUtils
import org.apache.flink.table.validate._

import scala.collection.JavaConversions._

case class Abs(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Abs")

  override def toString: String = s"abs($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ABS, child.toRexNode)
  }
}

case class Ceil(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Ceil")

  override def toString: String = s"ceil($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.CEIL, child.toRexNode)
  }
}

case class Exp(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"exp($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.EXP, child.toRexNode)
  }
}


case class Floor(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Floor")

  override def toString: String = s"floor($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.FLOOR, child.toRexNode)
  }
}

case class Log10(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"log10($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.LOG10, child.toRexNode)
  }
}

case class Log2(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(FlinkSqlOperatorTable.LOG2, child.toRexNode)
  }

  override def toString: String = s"log2($child)"
}

case class Cosh(child: PlannerExpression) extends UnaryExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(FlinkSqlOperatorTable.COSH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Cosh")

  override def toString = s"cosh($child)"
}

case class Log(base: PlannerExpression, antilogarithm: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {
  def this(antilogarithm: PlannerExpression) = this(E(), antilogarithm)

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def children: Seq[PlannerExpression] =
    if (base == null) Seq(antilogarithm) else Seq(base, antilogarithm)

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq.fill(children.length)(DOUBLE_TYPE_INFO)

  override def toString: String = s"log(${children.mkString(",")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.LOG, children.map(_.toRexNode))
  }
}

object Log {
  def apply(antilogarithm: PlannerExpression): Log = Log(null, antilogarithm)
}

case class Ln(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"ln($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.LN, child.toRexNode)
  }
}

case class Power(left: PlannerExpression, right: PlannerExpression)
  extends BinaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    DOUBLE_TYPE_INFO :: DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"pow($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.POWER, left.toRexNode, right.toRexNode)
  }
}

case class Sinh(child: PlannerExpression) extends UnaryExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO;

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Sinh")

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(FlinkSqlOperatorTable.SINH, child.toRexNode)
  }

  override def toString = s"sinh($child)"
}

case class Sqrt(child: PlannerExpression) extends UnaryExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(DOUBLE_TYPE_INFO)

  override def toString: String = s"sqrt($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.POWER, child.toRexNode, Literal(0.5).toRexNode)
  }
}

case class Sin(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Sin")

  override def toString: String = s"sin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.SIN, child.toRexNode)
  }
}

case class Cos(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Cos")

  override def toString: String = s"cos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.COS, child.toRexNode)
  }
}

case class Tan(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Tan")

  override def toString: String = s"tan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.TAN, child.toRexNode)
  }
}

case class Tanh(child: PlannerExpression) extends UnaryExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(FlinkSqlOperatorTable.TANH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Tanh")

  override def toString = s"tanh($child)"
}

case class Cot(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Cot")

  override def toString: String = s"cot($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.COT, child.toRexNode)
  }
}

case class Asin(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Asin")

  override def toString: String = s"asin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ASIN, child.toRexNode)
  }
}

case class Acos(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Acos")

  override def toString: String = s"acos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ACOS, child.toRexNode)
  }
}

case class Atan(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Atan")

  override def toString: String = s"atan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ATAN, child.toRexNode)
  }
}

case class Atan2(y: PlannerExpression, x: PlannerExpression) extends BinaryExpression {

  override private[flink] def left = y

  override private[flink] def right = x

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput() = {
    TypeInfoCheckUtils.assertNumericExpr(y.resultType, "atan2")
    TypeInfoCheckUtils.assertNumericExpr(x.resultType, "atan2")
  }

  override def toString: String = s"atan2($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ATAN2, left.toRexNode, right.toRexNode)
  }
}

case class Degrees(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Degrees")

  override def toString: String = s"degrees($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.DEGREES, child.toRexNode)
  }
}

case class Radians(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "Radians")

  override def toString: String = s"radians($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.RADIANS, child.toRexNode)
  }
}

case class Sign(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertNumericExpr(child.resultType, "sign")

  override def toString: String = s"sign($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.SIGN, child.toRexNode)
  }
}

case class Round(left: PlannerExpression, right: PlannerExpression)
  extends BinaryExpression {
  override private[flink] def resultType: TypeInformation[_] = left.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeInfoCheckUtils.isInteger(right.resultType)) {
      ValidationFailure(s"round right requires int, get " +
        s"$right : ${right.resultType}")
    }
    TypeInfoCheckUtils.assertNumericExpr(left.resultType, s"round left :$left")
  }

  override def toString: String = s"round($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.ROUND, left.toRexNode, right.toRexNode)
  }
}

case class Pi() extends LeafExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override def toString: String = s"pi()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.PI)
  }
}

case class E() extends LeafExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override def toString: String = s"e()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.E)
  }
}

case class Rand(seed: PlannerExpression) extends PlannerExpression with InputTypeSpec {

  def this() = this(null)

  override private[flink] def children: Seq[PlannerExpression] = if (seed != null) {
    seed :: Nil
  } else {
    Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: Nil
  } else {
    Nil
  }

  override def toString: String = if (seed != null) {
    s"rand($seed)"
  } else {
    s"rand()"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.RAND, children.map(_.toRexNode))
  }
}

case class RandInteger(seed: PlannerExpression, bound: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  def this(bound: PlannerExpression) = this(null, bound)

  override private[flink] def children: Seq[PlannerExpression] = if (seed != null) {
    seed :: bound :: Nil
  } else {
    bound :: Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.INT_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: INT_TYPE_INFO :: Nil
  } else {
    INT_TYPE_INFO :: Nil
  }

  override def toString: String = if (seed != null) {
    s"randInteger($seed, $bound)"
  } else {
    s"randInteger($bound)"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.RAND_INTEGER, children.map(_.toRexNode))
  }
}

case class Bin(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeInfoCheckUtils.assertIntegerFamilyExpr(child.resultType, "Bin")

  override def toString: String = s"bin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.BIN, child.toRexNode)
  }
}

case class Hex(child: PlannerExpression) extends UnaryExpression {
  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (TypeInfoCheckUtils.isIntegerFamily(child.resultType) ||
        TypeInfoCheckUtils.isString(child.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"hex() requires an integer or string input but was '${child.resultType}'.")
    }
  }

  override def toString: String = s"hex($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.HEX, child.toRexNode)
  }
}

case class UUID() extends LeafExpression {
  override private[flink] def resultType = BasicTypeInfo.STRING_TYPE_INFO

  override def toString: String = s"uuid()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.UUID)
  }
}

case class Truncate(base: PlannerExpression, num: PlannerExpression)
    extends PlannerExpression with InputTypeSpec {
  def this(base: PlannerExpression) = this(base, null)

  override private[flink] def resultType: TypeInformation[_] = base.resultType

  override private[flink] def children: Seq[PlannerExpression] =
    if (num == null) Seq(base) else Seq(base, num)

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    if (num == null) Seq(base.resultType) else Seq(base.resultType, INT_TYPE_INFO)

  override def toString: String = s"truncate(${children.mkString(",")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(FlinkSqlOperatorTable.TRUNCATE, children.map(_.toRexNode))
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (num != null) {
      if (!TypeInfoCheckUtils.isInteger(num.resultType)) {
        ValidationFailure(s"truncate num requires int, get " +
          s"$num : ${num.resultType}")
      }
    }
    TypeInfoCheckUtils.assertNumericExpr(base.resultType, s"truncate base :$base")
  }
}

object Truncate {
  def apply(base: PlannerExpression): Truncate = Truncate(base, null)
}

