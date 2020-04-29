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
package org.apache.flink.table.planner.expressions

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.`type`.FlinkReturnTypes
import org.apache.flink.table.planner.typeutils.TypeCoercion
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.{fromLogicalTypeToTypeInfo, fromTypeInfoToLogicalType}
import org.apache.flink.table.runtime.typeutils.{BigDecimalTypeInfo, DecimalDataTypeInfo}
import org.apache.flink.table.types.logical.{DecimalType, LogicalType}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeUtil

import scala.collection.JavaConverters._

object ReturnTypeInference {

  private lazy val typeSystem = new FlinkTypeSystem
  private lazy val typeFactory = new FlinkTypeFactory(typeSystem)

  /**
    * Infer resultType of [[Minus]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[org.apache.calcite.sql.type.ReturnTypes.NULLABLE_SUM]] which is the return type of
    * [[org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS]].
    *
    * @param minus minus Expression
    * @return result type
    */
  def inferMinus(minus: Minus): TypeInformation[_] = inferPlusOrMinus(minus)

  /**
    * Infer resultType of [[Plus]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[org.apache.calcite.sql.type.ReturnTypes.NULLABLE_SUM]] which is the return type of
    * * [[org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS]].
    *
    * @param plus plus Expression
    * @return result type
    */
  def inferPlus(plus: Plus): TypeInformation[_] = inferPlusOrMinus(plus)

  private def inferPlusOrMinus(op: BinaryArithmetic): TypeInformation[_] = {
    val decimalTypeInference = (
      leftType: RelDataType,
      rightType: RelDataType,
      wideResultType: LogicalType) => {
      if (SqlTypeUtil.isExactNumeric(leftType) &&
        SqlTypeUtil.isExactNumeric(rightType) &&
        (SqlTypeUtil.isDecimal(leftType) || SqlTypeUtil.isDecimal(rightType))) {
        val lp = leftType.getPrecision
        val ls = leftType.getScale
        val rp = rightType.getPrecision
        val rs = rightType.getScale
        val scale = Math.max(ls, rs)
        assert(scale <= typeSystem.getMaxNumericScale)
        var precision = Math.max(lp - ls, rp - rs) + scale + 1
        precision = Math.min(precision, typeSystem.getMaxNumericPrecision)
        assert(precision > 0)
        fromLogicalTypeToTypeInfo(wideResultType) match {
          case _: DecimalDataTypeInfo => DecimalDataTypeInfo.of(precision, scale)
          case _: BigDecimalTypeInfo => BigDecimalTypeInfo.of(precision, scale)
        }
      } else {
        val resultType = typeFactory.leastRestrictive(
          List(leftType, rightType).asJava)
        fromLogicalTypeToTypeInfo(FlinkTypeFactory.toLogicalType(resultType))
      }
    }
    inferBinaryArithmetic(op, decimalTypeInference, t => fromLogicalTypeToTypeInfo(t))
  }

  /**
    * Infer resultType of [[Mul]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[org.apache.calcite.sql.type.ReturnTypes.PRODUCT_NULLABLE]] which is the return type of
    * * * [[org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY]].
    *
    * @param mul mul Expression
    * @return result type
    */
  def inferMul(mul: Mul): TypeInformation[_] = {
    val decimalTypeInference = (
      leftType: RelDataType,
      rightType: RelDataType) => typeFactory.createDecimalProduct(leftType, rightType)
    inferDivOrMul(mul, decimalTypeInference)
  }

  /**
    * Infer resultType of [[Div]] expression.
    * The decimal type inference keeps consistent with
    * [[FlinkReturnTypes.FLINK_QUOTIENT_NULLABLE]] which
    * is the return type of [[FlinkSqlOperatorTable.DIVIDE]].
    *
    * @param div div Expression
    * @return result type
    */
  def inferDiv(div: Div): TypeInformation[_] = {
    val decimalTypeInference = (
      leftType: RelDataType,
      rightType: RelDataType) => typeFactory.createDecimalQuotient(leftType, rightType)
    inferDivOrMul(div, decimalTypeInference)
  }

  private def inferDivOrMul(
      op: BinaryArithmetic,
      decimalTypeInfer: (RelDataType, RelDataType) => RelDataType
  ): TypeInformation[_] = {
    val decimalFunc = (
      leftType: RelDataType,
      rightType: RelDataType,
      _: LogicalType) => {
      val decimalType = decimalTypeInfer(leftType, rightType)
      if (decimalType != null) {
        fromLogicalTypeToTypeInfo(FlinkTypeFactory.toLogicalType(decimalType))
      } else {
        val resultType = typeFactory.leastRestrictive(
          List(leftType, rightType).asJava)
        fromLogicalTypeToTypeInfo(FlinkTypeFactory.toLogicalType(resultType))
      }
    }
    val nonDecimalType = (t: LogicalType) => fromLogicalTypeToTypeInfo(t)
    inferBinaryArithmetic(op, decimalFunc, nonDecimalType)
  }

  private def inferBinaryArithmetic(
      binaryOp: BinaryArithmetic,
      decimalInfer: (RelDataType, RelDataType, LogicalType) => TypeInformation[_],
      nonDecimalInfer: LogicalType => TypeInformation[_]
  ): TypeInformation[_] = {
    val leftType = fromTypeInfoToLogicalType(binaryOp.left.resultType)
    val rightType = fromTypeInfoToLogicalType(binaryOp.right.resultType)
    TypeCoercion.widerTypeOf(leftType, rightType) match {
      case Some(t: DecimalType) =>
        val leftRelDataType = typeFactory.createFieldTypeFromLogicalType(leftType)
        val rightRelDataType = typeFactory.createFieldTypeFromLogicalType(rightType)
        decimalInfer(leftRelDataType, rightRelDataType, t)
      case Some(t) => nonDecimalInfer(t)
      case None => throw new TableException("This will not happen here!")
    }
  }

  /**
    * Infer resultType of [[Round]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[FlinkReturnTypes]].ROUND_FUNCTION_NULLABLE
    *
    * @param round round Expression
    * @return result type
    */
  def inferRound(round: Round): TypeInformation[_] = {
    val numType = round.left.resultType
    numType match {
      case _: DecimalDataTypeInfo | _: BigDecimalTypeInfo =>
        val lenValue = round.right match {
          case Literal(v: Int, BasicTypeInfo.INT_TYPE_INFO) => v
          case _ => throw new TableException("This will not happen here!")
        }
        val numLogicalType = fromTypeInfoToLogicalType(numType)
        val numRelDataType = typeFactory.createFieldTypeFromLogicalType(numLogicalType)
        val p = numRelDataType.getPrecision
        val s = numRelDataType.getScale
        val dt = FlinkTypeSystem.inferRoundType(p, s, lenValue)
        fromLogicalTypeToTypeInfo(dt)
      case t => t
    }
  }

  /**
    * Infer resultType of [[Floor]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[org.apache.calcite.sql.type.ReturnTypes]].ARG0_OR_EXACT_NO_SCALE
    *
    * @param floor floor Expression
    * @return result type
    */
  def inferFloor(floor: Floor): TypeInformation[_] = getArg0OrExactNoScale(floor)

  /**
    * Infer resultType of [[Ceil]] expression.
    * The decimal type inference keeps consistent with Calcite
    * [[org.apache.calcite.sql.type.ReturnTypes]].ARG0_OR_EXACT_NO_SCALE
    *
    * @param ceil ceil Expression
    * @return result type
    */
  def inferCeil(ceil: Ceil): TypeInformation[_] = getArg0OrExactNoScale(ceil)

  private def getArg0OrExactNoScale(op: UnaryExpression) = {
    val childType = op.child.resultType
    childType match {
      case t: DecimalDataTypeInfo => DecimalDataTypeInfo.of(t.precision(), 0)
      case t: BigDecimalTypeInfo => BigDecimalTypeInfo.of(t.precision(), 0)
      case _ => childType
    }
  }

}
