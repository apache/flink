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

import org.apache.flink.api.table.expressions.analysis.SelectionAnalyzer
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.java.aggregation.Aggregations

import scala.collection.mutable

/**
 * This is used to expand a [[Select]] that contains aggregations. If it is called on a [[Select]]
 * without aggregations it is simply returned.
 *
 * This select:
 * {{{
 *   in.select('key, 'value.avg)
 * }}}
 *
 * is transformed to this expansion:
 * {{{
 *   in
 *     .select('key, 'value, Literal(1) as 'intermediate.1)
 *     .aggregate('value.sum, 'intermediate.1.sum)
 *     .select('key, 'value / 'intermediate.1)
 * }}}
 *
 * If the input of the [[Select]] is a [[GroupBy]] this is preserved before the aggregation.
 */
object ExpandAggregations {
  def apply(select: Select): PlanNode = select match {
    case Select(input, selection) =>

      val aggregations = mutable.HashMap[(Expression, Aggregations), String]()
      val intermediateFields = mutable.HashSet[Expression]()
      val aggregationIntermediates = mutable.HashMap[Aggregation, Seq[Expression]]()

      var intermediateCount = 0
      var resultCount = 0
      selection foreach {  f =>
        f.transformPre {
          case agg: Aggregation =>
            val intermediateReferences = agg.getIntermediateFields.zip(agg.getAggregations) map {
              case (expr, basicAgg) =>
                resultCount += 1
                val resultName = s"result.$resultCount"
                aggregations.get((expr, basicAgg)) match {
                  case Some(intermediateName) =>
                    Naming(ResolvedFieldReference(intermediateName, expr.typeInfo), resultName)
                  case None =>
                    intermediateCount = intermediateCount + 1
                    val intermediateName = s"intermediate.$intermediateCount"
                    intermediateFields += Naming(expr, intermediateName)
                    aggregations((expr, basicAgg)) = intermediateName
                    Naming(ResolvedFieldReference(intermediateName, expr.typeInfo), resultName)
                }
            }

            aggregationIntermediates(agg) = intermediateReferences
            // Return a NOP so that we don't add the children of the aggregation
            // to intermediate fields. We already added the necessary fields to the list
            // of intermediate fields.
            NopExpression()

          case fa: ResolvedFieldReference =>
            if (!fa.name.startsWith("intermediate")) {
              intermediateFields += Naming(fa, fa.name)
            }
            fa
        }
      }

      if (aggregations.isEmpty) {
        // no aggregations, just return
        return select
      }

      // also add the grouping keys to the set of intermediate fields, because we use a Set,
      // they are only added when not already present
      input match {
        case GroupBy(_, groupingFields) =>
          groupingFields foreach {
            case fa: ResolvedFieldReference =>
              intermediateFields += Naming(fa, fa.name)
          }
        case _ => // Nothing to add
      }

      val basicAggregations = aggregations.map {
        case ((expr, basicAgg), fieldName) =>
          (fieldName, basicAgg)
      }

      val finalFields = selection.map {  f =>
        f.transformPre {
          case agg: Aggregation =>
            val intermediates = aggregationIntermediates(agg)
            agg.getFinalField(intermediates)
        }
      }

      val intermediateAnalyzer = new SelectionAnalyzer(input.outputFields)
      val analyzedIntermediates = intermediateFields.toSeq.map(intermediateAnalyzer.analyze)

      val finalAnalyzer =
        new SelectionAnalyzer(analyzedIntermediates.map(e => (e.name, e.typeInfo)))
      val analyzedFinals = finalFields.map(finalAnalyzer.analyze)

      val result = input match {
        case GroupBy(groupByInput, groupingFields) =>
          Select(
            Aggregate(
              GroupBy(
                Select(groupByInput, analyzedIntermediates),
                groupingFields),
              basicAggregations.toSeq),
            analyzedFinals)

        case _ =>
          Select(
            Aggregate(
              Select(input, analyzedIntermediates),
              basicAggregations.toSeq),
            analyzedFinals)

      }

      result

    case _ => select
  }
}
