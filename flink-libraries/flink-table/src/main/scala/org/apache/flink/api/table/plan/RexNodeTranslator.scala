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

package org.apache.flink.api.table.plan

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.flink.api.table.expressions._

import scala.collection.JavaConversions._

object RexNodeTranslator {

  /**
    * Extracts all aggregation expressions (zero, one, or more) from an expression, translates
    * these aggregation expressions into Calcite AggCalls, and replaces the original aggregation
    * expressions by field accesses expressions.
    */
  def extractAggCalls(exp: Expression, relBuilder: RelBuilder): Pair[Expression, List[AggCall]] = {

    exp match {
      case agg: Aggregation =>
        val name = TranslationContext.getUniqueName
        val aggCall = toAggCall(agg, name, relBuilder)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case n@Naming(agg: Aggregation, name) =>
        val aggCall = toAggCall(agg, name, relBuilder)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case l: LeafExpression =>
        (l, Nil)
      case u: UnaryExpression =>
        val c = extractAggCalls(u.child, relBuilder)
        (u.makeCopy(List(c._1)), c._2)
      case b: BinaryExpression =>
        val l = extractAggCalls(b.left, relBuilder)
        val r = extractAggCalls(b.right, relBuilder)
        (b.makeCopy(List(l._1, r._1)), l._2 ::: r._2)

      // Scalar functions
      case c@Call(name, args@_*) =>
        val newArgs = args.map(extractAggCalls(_, relBuilder)).toList
        (c.makeCopy(name :: newArgs.map(_._1)), newArgs.flatMap(_._2))

      case e@AnyRef =>
        throw new IllegalArgumentException(
          s"Expression $e of type ${e.getClass} not supported yet")
    }
  }

  /**
    * Translates a Table API expression into a Calcite RexNode.
    */
  def toRexNode(exp: Expression, relBuilder: RelBuilder): RexNode = {

    exp match {
      // Basic operators
      case Literal(value, tpe) =>
        relBuilder.literal(value)
      case ResolvedFieldReference(name, tpe) =>
        relBuilder.field(name)
      case UnresolvedFieldReference(name) =>
        relBuilder.field(name)
      case NopExpression() =>
        throw new IllegalArgumentException("NoOp expression encountered")
      case Naming(child, name) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.alias(c, name)
      case Cast(child, tpe) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.cast(c, TypeConverter.typeInfoToSqlType(tpe))
      case Not(child) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.not(c)
      case Or(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.or(l, r)
      case And(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.and(l, r)
      case EqualTo(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.equals(l, r)
      case NotEqualTo(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.not(relBuilder.equals(l, r))
        relBuilder.call(SqlStdOperatorTable.NOT_EQUALS, l, r)
      case LessThan(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.LESS_THAN, l, r)
      case LessThanOrEqual(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, l, r)
      case GreaterThan(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.GREATER_THAN, l, r)
      case GreaterThanOrEqual(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, l, r)
      case IsNull(child) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.isNull(c)
      case IsNotNull(child) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.isNotNull(c)
      case Plus(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.PLUS, l, r)
      case Minus(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.MINUS, l, r)
      case Mul(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.MULTIPLY, l, r)
      case Div(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.DIVIDE, l, r)
      case Mod(left, right) =>
        val l = toRexNode(left, relBuilder)
        val r = toRexNode(right, relBuilder)
        relBuilder.call(SqlStdOperatorTable.MOD, l, r)
      case UnaryMinus(child) =>
        val c = toRexNode(child, relBuilder)
        relBuilder.call(SqlStdOperatorTable.UNARY_MINUS, c)

      // Scalar functions
      case Call(name, args@_*) =>
        val rexArgs = args.map(toRexNode(_, relBuilder))
        val sqlOperator = toSqlOperator(name)
        relBuilder.call(sqlOperator, rexArgs)

      case a: Aggregation =>
        throw new IllegalArgumentException(s"Aggregation expression $a not allowed at this place")
      case e@AnyRef =>
        throw new IllegalArgumentException(
          s"Expression $e of type ${e.getClass} not supported yet")
    }
  }

  private def toAggCall(agg: Aggregation, name: String, relBuilder: RelBuilder): AggCall = {

    val rexNode = toRexNode(agg.child, relBuilder)
    agg match {
      case s: Sum => relBuilder.aggregateCall(
        SqlStdOperatorTable.SUM, false, null, name, rexNode)
      case m: Min => relBuilder.aggregateCall(
        SqlStdOperatorTable.MIN, false, null, name, rexNode)
      case m: Max => relBuilder.aggregateCall(
        SqlStdOperatorTable.MAX, false, null, name, rexNode)
      case c: Count => relBuilder.aggregateCall(
        SqlStdOperatorTable.COUNT, false, null, name, rexNode)
      case a: Avg => relBuilder.aggregateCall(
        SqlStdOperatorTable.AVG, false, null, name, rexNode)
    }
  }

  private def toSqlOperator(name: String): SqlOperator = {
    name match {
      case BuiltInFunctionNames.SUBSTRING => SqlStdOperatorTable.SUBSTRING
      case BuiltInFunctionNames.TRIM => SqlStdOperatorTable.TRIM
      case BuiltInFunctionNames.CHAR_LENGTH => SqlStdOperatorTable.CHAR_LENGTH
      case BuiltInFunctionNames.UPPER_CASE => SqlStdOperatorTable.UPPER
      case BuiltInFunctionNames.LOWER_CASE => SqlStdOperatorTable.LOWER
      case BuiltInFunctionNames.INIT_CAP => SqlStdOperatorTable.INITCAP
      case BuiltInFunctionNames.LIKE => SqlStdOperatorTable.LIKE
      case BuiltInFunctionNames.SIMILAR => SqlStdOperatorTable.SIMILAR_TO
      case _ => ???
    }
  }

}
