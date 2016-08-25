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
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql._
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.api.table.typeutils.TypeCheckUtils
import org.apache.flink.api.table.validate.{ValidationFailure, ValidationSuccess}

abstract sealed class Aggregation extends UnaryExpression {

  override def toString = s"Aggregate($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")

  /**
    * Convert Aggregate to its counterpart in Calcite, i.e. AggCall
    */
  private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall
}

case class Sum(child: Expression) extends Aggregation {
  override def toString = s"sum($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.SUM, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")
}

case class Min(child: Expression) extends Aggregation {
  override def toString = s"min($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MIN, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")
}

case class Max(child: Expression) extends Aggregation {
  override def toString = s"max($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.MAX, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")
}

case class Count(child: Expression) extends Aggregation {
  override def toString = s"count($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO
}

case class Avg(child: Expression) extends Aggregation {
  override def toString = s"avg($child)"

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(SqlStdOperatorTable.AVG, false, null, name, child.toRexNode)
  }

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")
}

// ------------------------------------------------------------------------------------------------

abstract class WindowProperty(child: Expression) extends Aggregation {

  override private[flink] def resultType = SqlTimeTypeInfo.TIMESTAMP

  override private[flink] def validateInput() =
    if (child.isInstanceOf[WindowReference]) {
      ValidationSuccess
    } else {
      ValidationFailure("Child must be a window reference.")
    }
}

object WindowProperty {

  private[flink] class WindowStartSqlAggFunction
    extends SqlAggFunction(
      "WINDOW_START",
      new SqlIdentifier("WINDOW_START", SqlParserPos.ZERO),
      SqlKind.OTHER,
      ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
      null,
      OperandTypes.NILADIC,
      SqlFunctionCategory.SYSTEM,
      false,
      false)

  private[flink] class WindowEndSqlAggFunction
    extends SqlAggFunction(
      "WINDOW_END",
      new SqlIdentifier("WINDOW_END", SqlParserPos.ZERO),
      SqlKind.OTHER,
      ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
      null,
      OperandTypes.NILADIC,
      SqlFunctionCategory.SYSTEM,
      false,
      false)

  private[flink] val StartSqlAggFunction = new WindowStartSqlAggFunction()

  private[flink] val EndSqlAggFunction = new WindowEndSqlAggFunction()
}

case class WindowStart(child: Expression) extends WindowProperty(child) {

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      WindowProperty.StartSqlAggFunction,
      false,
      null,
      name,
      relBuilder.literal(-1)) // this a dummy argument as aggregation must have at least one
  }

  override def toString: String = s"start($child)"
}

case class WindowEnd(child: Expression) extends WindowProperty(child) {

  override private[flink] def toAggCall(name: String)(implicit relBuilder: RelBuilder): AggCall = {
    relBuilder.aggregateCall(
      WindowProperty.EndSqlAggFunction,
      false,
      null,
      name,
      relBuilder.literal(-1)) // this a dummy argument as aggregation must have at least one
  }

  override def toString: String = s"end($child)"
}
