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
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.utils.AggSqlFunction
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulateMethodSignature, signatureToString, signaturesToString}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract sealed class Aggregation extends Expression {

  override def toString = s"Aggregate"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall

  /**
    * Returns the SqlAggFunction for this Aggregation.
    */
  private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder): SqlAggFunction

}

case class Sum(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.SUM, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val returnType = relBuilder
      .getTypeFactory.asInstanceOf[FlinkTypeFactory]
      .createTypeFromTypeInfo(resultType)
    new SqlSumAggFunction(returnType)
  }
}

case class Min(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"min($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MIN, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlMinMaxAggFunction(MIN)
  }
}

case class Max(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"max($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MAX, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlMinMaxAggFunction(MAX)
  }
}

case class Count(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"count($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlCountAggFunction()
  }
}

case class Avg(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.AVG, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    new SqlAvgAggFunction(AVG)
  }
}

case class UDAGGFunctionCall(
    aggregateFunction: AggregateFunction[_, _],
    args: Seq[Expression])
  extends Aggregation {

  override private[flink] def children: Seq[Expression] = args

  // Override makeCopy method in TreeNode, to produce vargars properly
  override def makeCopy(args: Array[AnyRef]): this.type = {
    if (args.length < 1) {
      throw new TableException("Invalid constructor params")
    }
    val agg = args.head.asInstanceOf[AggregateFunction[_, _]]
    val arg = args.last.asInstanceOf[Seq[Expression]]
    new UDAGGFunctionCall(agg, arg).asInstanceOf[this.type]
  }

  override def resultType: TypeInformation[_] = TypeExtractor.createTypeInfo(
    aggregateFunction, classOf[AggregateFunction[_, _]], aggregateFunction.getClass, 0)

  override def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    val foundSignature = getAccumulateMethodSignature(aggregateFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${signatureToString(signature)} \n" +
                          s"Expected: ${signaturesToString(aggregateFunction, "accumulate")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString(): String = s"${aggregateFunction.getClass.getSimpleName}($args)"

  override def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = AggSqlFunction(name, aggregateFunction, resultType, typeFactory)
    relBuilder.aggregateCall(sqlFunction, false, null, name, args.map(_.toRexNode): _*)
  }

  override private[flink] def getSqlAggFunction()(implicit relBuilder: RelBuilder) = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    AggSqlFunction("UDAGG", // tableAPI parser does not really use this
                   aggregateFunction,
                   resultType,
                   typeFactory)
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    relBuilder.call(
      AggSqlFunction("UDAGG", // tableAPI parser does not really use this
                     aggregateFunction,
                     resultType,
                     typeFactory),
      args.map(_.toRexNode): _*)
  }
}
