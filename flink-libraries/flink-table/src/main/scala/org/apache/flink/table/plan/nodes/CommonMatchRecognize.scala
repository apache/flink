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

package org.apache.flink.table.plan.nodes

import java.util.{List => JList, SortedSet => JSortedSet}

import com.google.common.collect.ImmutableMap
import org.apache.calcite.rel.{RelCollation, RelWriter}
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.AfterOption
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.runtime.aggregate.SortUtil.directionToOrder

import scala.collection.JavaConverters._

trait CommonMatchRecognize {

  private def partitionKeysToString(
      keys: JList[RexNode],
      fieldNames: Seq[String],
      expression: (RexNode, Seq[String], Option[Seq[RexNode]]) => String)
    : String =
    keys.asScala.map(k => expression(k, fieldNames, None)).mkString(", ")

  private def orderingToString(orders: RelCollation, fieldNames: Seq[String]): String =
    orders.getFieldCollations.asScala.map {
      x => s"${fieldNames(x.getFieldIndex)} ${directionToOrder(x.direction).getShortName}"
    }.mkString(", ")

  private def measuresDefineToString(
      measures: ImmutableMap[String, RexNode],
      fieldNames: Seq[String],
      expression: (RexNode, Seq[String], Option[Seq[RexNode]]) => String)
    : String =
    measures.asScala.map {
      case (k, v) => s"${expression(v, fieldNames, None)} AS $k"
    }.mkString(", ")

  private def rowsPerMatchToString(isAll: Boolean): String =
    if (isAll) "ALL ROWS PER MATCH" else "ONE ROW PER MATCH"

  private def subsetToString(subset: ImmutableMap[String, JSortedSet[String]]): String =
    subset.asScala.map {
      case (k, v) => s"$k = (${v.asScala.mkString(", ")})"
    }.mkString(", ")

  private def afterMatchToString(
      after: RexNode,
      fieldNames: Seq[String])
    : String =
    after.getKind match {
      case SqlKind.SKIP_TO_FIRST => s"SKIP TO FIRST ${
        after.asInstanceOf[RexCall].operands.get(0).toString
      }"
      case SqlKind.SKIP_TO_LAST => s"SKIP TO LAST ${
        after.asInstanceOf[RexCall].operands.get(0).toString
      }"
      case SqlKind.LITERAL => after.asInstanceOf[RexLiteral]
        .getValueAs(classOf[AfterOption]) match {
        case AfterOption.SKIP_PAST_LAST_ROW => "SKIP PAST LAST ROW"
        case AfterOption.SKIP_TO_NEXT_ROW => "SKIP TO NEXT ROW"
      }
      case _ => throw new IllegalStateException(s"Corrupted query tree. Unexpected $after for " +
        s"after match strategy.")
    }

  private[flink] def matchToString(
      logicalMatch: MatchRecognize,
      fieldNames: Seq[String],
      expression: (RexNode, Seq[String], Option[Seq[RexNode]]) => String)
    : String = {
    val partitionBy = if (!logicalMatch.partitionKeys.isEmpty) {
      s"PARTITION BY: ${
        partitionKeysToString(logicalMatch.partitionKeys, fieldNames, expression)
      }"
    } else {
      ""
    }

    val orderBy = if (!logicalMatch.orderKeys.getFieldCollations.isEmpty) {
      s"ORDER BY: ${orderingToString(logicalMatch.orderKeys, fieldNames)}"
    } else {
      ""
    }

    val measures = if (!logicalMatch.measures.isEmpty) {
      s"MEASURES: ${measuresDefineToString(logicalMatch.measures, fieldNames, expression)}"
    } else {
      ""
    }

    val afterMatch = s"${afterMatchToString(logicalMatch.after, fieldNames)}"

    val allRows = rowsPerMatchToString(logicalMatch.allRows)

    val pattern = s"PATTERN: (${logicalMatch.pattern.toString})"

    val subset = if (!logicalMatch.subsets.isEmpty) {
      s"SUBSET: ${subsetToString(logicalMatch.subsets)} "
    } else {
      ""
    }

    val define = s"DEFINE: ${
      measuresDefineToString(logicalMatch.patternDefinitions, fieldNames, expression)
    }"

    val body = Seq(partitionBy, orderBy, measures, allRows, afterMatch, pattern, subset, define)
      .filterNot(_.isEmpty)
      .mkString(", ")

    s"Match($body)"
  }

  private[flink] def explainMatch(
      pw: RelWriter,
      logicalMatch: MatchRecognize,
      fieldNames: Seq[String],
      expression: (RexNode, Seq[String], Option[Seq[RexNode]]) => String)
    : RelWriter = {
    pw.itemIf("partitionBy",
      partitionKeysToString(logicalMatch.partitionKeys, fieldNames, expression),
      !logicalMatch.partitionKeys.isEmpty)
      .itemIf("orderBy",
        orderingToString(logicalMatch.orderKeys, fieldNames),
        !logicalMatch.orderKeys.getFieldCollations.isEmpty)
      .itemIf("measures",
        measuresDefineToString(logicalMatch.measures, fieldNames, expression),
        !logicalMatch.measures.isEmpty)
      .item("rowsPerMatch", rowsPerMatchToString(logicalMatch.allRows))
      .item("after", afterMatchToString(logicalMatch.after, fieldNames))
      .item("pattern", logicalMatch.pattern.toString)
      .itemIf("subset",
        subsetToString(logicalMatch.subsets),
        !logicalMatch.subsets.isEmpty)
      .item("define", logicalMatch.patternDefinitions)
  }
}
