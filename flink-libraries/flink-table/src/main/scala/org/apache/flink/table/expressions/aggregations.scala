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

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.table.api.types.{DataType, DataTypes, InternalType, MultisetType}
import org.apache.flink.table.api.functions.AggregateFunction
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.functions.sql.AggSqlFunctions
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.logical.LogicalExprVisitor
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract sealed class Aggregation extends Expression {

  override def toString = s"Aggregate"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(
      name: String,
      isDistinct: Boolean = false
  )(implicit relBuilder: RelBuilder): AggCall

  /**
    * Returns the SqlAggFunction for this Aggregation.
    */
  private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder): SqlAggFunction

}

// build-in Aggregations
case class DistinctAgg(child: Expression) extends Aggregation {

  private[flink] def distinct: Expression = DistinctAgg(child)

  override private[flink] def resultType: InternalType = child.resultType

  override private[flink] def validateInput(): ValidationResult = {
    super.validateInput()
    child match {
      case agg: Aggregation =>
        child.validateInput()
      case _ =>
        ValidationFailure(s"Distinct modifier cannot be applied to $child! " +
            s"It can only be applied to an aggregation expression, for example, " +
            s"'a.count.distinct which is equivalent with COUNT(DISTINCT a).")
    }
  }

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = true)(implicit relBuilder: RelBuilder) = {
    child.asInstanceOf[Aggregation].toAggCall(name, isDistinct = true)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    child.asInstanceOf[Aggregation].getSqlAggFunction()
  }

  override private[flink] def children = Seq(child)

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sum(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.SUM,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveSumType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val returnType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromInternalType(resultType, isNullable = true)
    new SqlSumAggFunction(returnType)
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Sum0(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum0($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.SUM0,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveSumType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum0")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.SUM0

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class IncrSum(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"incr_sum($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      AggSqlFunctions.INCR_SUM,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveSumType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "incr_sum")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    AggSqlFunctions.INCR_SUM

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Min(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"min($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.MIN,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.MIN

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Max(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"max($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.MAX,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.MAX

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Count(child: Expression) extends Aggregation {
  private[flink] val isWildcard = child.checkEquals(Literal("*"))
  override private[flink] def children: Seq[Expression] = if (isWildcard) Seq() else Seq(child)
  override def toString = s"count($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    if (isWildcard) {
      relBuilder.aggregateCall(
        SqlStdOperatorTable.COUNT,
        isDistinct,
        false,
        null,
        name)
    } else {
      relBuilder.aggregateCall(
        SqlStdOperatorTable.COUNT,
        isDistinct,
        false,
        null,
        name,
        child.toRexNode)
    }

  override private[flink] def resultType = DataTypes.LONG

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.COUNT

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Avg(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.AVG,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    SqlStdOperatorTable.AVG
  }

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

/**
  * Returns a multiset aggregates.
  */
case class Collect(child: Expression) extends Aggregation  {

  override private[flink] def children: Seq[Expression] = Seq(child)

  override private[flink] def resultType: InternalType =
    new MultisetType(child.resultType)

  override def toString: String = s"collect($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.COLLECT,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.COLLECT

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Rank() extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq()
  override def toString = s"rank()"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.RANK,
      isDistinct,
      false,
      null,
      name)

  override private[flink] def resultType = DataTypes.LONG

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.RANK

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class DenseRank() extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq()
  override def toString = s"dense_rank()"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.DENSE_RANK,
      isDistinct,
      false,
      null,
      name)

  override private[flink] def resultType = DataTypes.LONG

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.DENSE_RANK

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class RowNumber() extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq()
  override def toString = s"row_number()"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.ROW_NUMBER,
      isDistinct,
      false,
      null,
      name)

  override private[flink] def resultType = DataTypes.LONG

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.ROW_NUMBER

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Lead(exp: Expression, offset: Literal = Literal(1),
  var default: Expression = null) extends Aggregation {
  private[flink] var offsetValue: Long = _

  override private[flink] def children: Seq[Expression] = if (default == null) {
    Seq(exp, offset)
  } else {
    Seq(exp, offset, default)
  }

  override def toString = s"lead($exp, $offset, $default)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.LEAD,
      isDistinct,
      false,
      null,
      name,
      children.map(_.toRexNode): _*)

  override private[flink] def resultType = exp.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (default == null) {
      default = Null(exp.resultType)
    }
    if (exp.resultType != default.resultType) {
      ValidationFailure("Expression and default value must have the same type.")
    } else {
      offset.value match {
        case o: Int =>
          offsetValue = o.toLong
          ValidationSuccess
        case o: Long =>
          offsetValue = o
          ValidationSuccess
        case _ => ValidationFailure("Lead offset must be an integer.")
      }
    }
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.LEAD

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  def getOffsetValue: Long = offsetValue
}

case class Lag(exp: Expression, offset: Literal = Literal(1),
  var default: Expression = null) extends Aggregation {
  private[flink] var offsetValue: Long = _

  override private[flink] def children: Seq[Expression] = if (default == null) {
    Seq(exp, offset)
  } else {
    Seq(exp, offset, default)
  }

  override def toString = s"lag($exp, $offset, $default)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.LAG,
      isDistinct,
      false,
      null,
      name,
      children.map(_.toRexNode): _*)

  override private[flink] def resultType = exp.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (default == null) {
      default = Null(exp.resultType)
    }
    if (exp.resultType != default.resultType) {
      ValidationFailure("Expression and default value must have the same type.")
    } else {
      offset.value match {
        case o: Int =>
          offsetValue = o.toLong
          ValidationSuccess
        case o: Long =>
          offsetValue = o
          ValidationSuccess
        case _ => ValidationFailure("Lag offset must be an integer.")
      }
    }
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.LAG

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)

  def getOffsetValue: Long = offsetValue
}

case class StddevPop(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"stddev_pop($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.STDDEV_POP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "stddev_pop")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.STDDEV_POP

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class StddevSamp(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"stddev_samp($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.STDDEV_SAMP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "stddev_samp")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.STDDEV_SAMP

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Stddev(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"stddev($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.STDDEV,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "stddev")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.STDDEV

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class VarPop(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"var_pop($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.VAR_POP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "var_pop")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.VAR_POP

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class VarSamp(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"var_samp($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.VAR_SAMP,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "var_samp")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.VAR_SAMP

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class Variance(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"variance($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      SqlStdOperatorTable.VARIANCE,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = FlinkTypeSystem.deriveAvgAggType(child.resultType)

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "variance")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    SqlStdOperatorTable.VARIANCE

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class FirstValue(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"first_value($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      AggSqlFunctions.FIRST_VALUE,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = child.resultType

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    AggSqlFunctions.FIRST_VALUE

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class LastValue(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"last_value($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      AggSqlFunctions.LAST_VALUE,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  override private[flink] def resultType = child.resultType

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    AggSqlFunctions.LAST_VALUE

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class SingleValue(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"single_value($child)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      getSqlAggFunction,
      isDistinct,
      false,
      null,
      name,
      child.toRexNode)

  def getCalciteType(relBuilder: RelBuilder): RelDataType =
    relBuilder
        .getTypeFactory.asInstanceOf[FlinkTypeFactory]
        .createTypeFromInternalType(resultType, isNullable = true)

  override private[flink] def resultType = child.resultType

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    new SqlSingleValueAggFunction(getCalciteType(relBuilder))

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

case class ConcatAgg(child: Expression, separator: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child, separator)
  override def toString = s"concat_agg($child, $separator)"

  override private[flink] def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      getSqlAggFunction,
      isDistinct,
      false,
      null,
      name,
      separator.toRexNode,
      child.toRexNode)

  override private[flink] def resultType = child.resultType

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) =
    AggSqlFunctions.CONCAT_AGG

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}

//Aggregate function calls

/**
  * Represent aggregate function call
  */
case class AggFunctionCall(
    aggregateFunction: AggregateFunction[_, _],
    externalResultType: DataType,
    externalAccType: DataType,
    args: Seq[Expression])
  extends Aggregation {

  override private[flink] def children: Seq[Expression] = args

  override def resultType: InternalType = externalResultType.toInternalType

  override def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    val foundSignature = getAccumulateMethodSignature(aggregateFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${signatureToString(signature)} \n" +
                          s"Expected: ${
                            getMethodSignatures(aggregateFunction, "accumulate").drop(1)
                              .map(signatureToString).mkString(", ")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = s"${aggregateFunction.getClass.getSimpleName}($args)"

  override def toAggCall(
      name: String, isDistinct: Boolean = false)(implicit relBuilder: RelBuilder): AggCall =
    relBuilder.aggregateCall(
      this.getSqlAggFunction(),
      isDistinct,
      false,
      null,
      name,
      args.map(_.toRexNode): _*)

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    AggSqlFunction(
      aggregateFunction.functionIdentifier,
      aggregateFunction.toString,
      aggregateFunction,
      externalResultType,
      externalAccType,
      typeFactory,
      aggregateFunction.requiresOver)
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    relBuilder.call(this.getSqlAggFunction(), args.map(_.toRexNode): _*)

  override def accept[T](logicalExprVisitor: LogicalExprVisitor[T]): T =
    logicalExprVisitor.visit(this)
}
