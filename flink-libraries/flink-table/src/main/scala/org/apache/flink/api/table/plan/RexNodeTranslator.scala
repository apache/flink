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

import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall

import org.apache.flink.api.table.expressions._

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
        val aggCall = agg.toAggCall(name)(relBuilder)
        val fieldExp = new UnresolvedFieldReference(name)
        (fieldExp, List(aggCall))
      case n@Naming(agg: Aggregation, name) =>
        val aggCall = agg.toAggCall(name)(relBuilder)
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
}
