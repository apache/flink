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

package org.apache.flink.table.plan.nodes.common

import java.util.{List => JList, SortedSet => JSortedSet}

import com.google.common.collect.ImmutableMap
import org.apache.calcite.rel.{RelCollation, RelWriter}
import org.apache.calcite.rex.{RexCall, RexLiteral, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlMatchRecognize.AfterOption
import org.apache.flink.table.plan.logical.MatchRecognize
import org.apache.flink.table.plan.util.SortUtil.directionToOrder

import scala.collection.JavaConverters._

trait CommonMatchRecognize {

  private def partitionKeysToString(
      keys: JList[RexNode],
      fieldNames: List[String],
      expression: (RexNode, List[String], Option[List[RexNode]]) => String)
    : String =
    keys.asScala.map(k => expression(k, fieldNames, None)).mkString(", ")

  private def orderingToString(orders: RelCollation, fieldNames: Seq[String]): String =
    orders.getFieldCollations.asScala.map {
      x => s"${fieldNames(x.getFieldIndex)} ${directionToOrder(x.direction).getShortName}"
    }.mkString(", ")

  private def measuresDefineToString(
      measures: ImmutableMap[String, RexNode],
      fieldNames: List[String],
      expression: (RexNode, List[String], Option[List[RexNode]]) => String)
    : String =
    measures.asScala.map {
      case (k, v) => s"${expression(v, fieldNames, None)} AS $k"
    }.mkString(", ")

  private def subsetToString(subset: ImmutableMap[String, JSortedSet[String]]): String =
    subset.asScala.map {
      case (k, v) => s"$k = (${v.asScala.mkString(", ")})"
    }.mkString(", ")

  private def afterMatchToString(
      after: RexNode,
      fieldNames: List[String])
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
      fieldNames: List[String],
      expression: (RexNode, List[String], Option[List[RexNode]]) => String)
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

    val allRows = if (logicalMatch.rowsPerMatch != null) {
      logicalMatch.rowsPerMatch.toString
    } else {
      ""
    }

    val pattern = s"PATTERN: (${logicalMatch.pattern.toString})"

    val interval = if (logicalMatch.interval != null) {
      logicalMatch.interval.toString
    } else {
      ""
    }

    val subset = if (!logicalMatch.subsets.isEmpty) {
      s"SUBSET: ${subsetToString(logicalMatch.subsets)} "
    } else {
      ""
    }

    val define = s"DEFINE: ${
      measuresDefineToString(logicalMatch.patternDefinitions, fieldNames, expression)
    }"

    val body =
      Seq(partitionBy, orderBy, measures, allRows, afterMatch, pattern, interval, subset, define)
        .filterNot(_.isEmpty)
        .mkString(", ")

    s"Match($body)"
  }

  private[flink] def explainMatch(
      pw: RelWriter,
      logicalMatch: MatchRecognize,
      fieldNames: List[String],
      expression: (RexNode, List[String], Option[List[RexNode]]) => String)
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
      .itemIf("rowsPerMatch", logicalMatch.rowsPerMatch, logicalMatch.rowsPerMatch != null)
      .item("after", afterMatchToString(logicalMatch.after, fieldNames))
      .item("pattern", logicalMatch.pattern.toString)
      .itemIf("within interval",
        if (logicalMatch.interval != null) {
          logicalMatch.interval.toString
        } else {
          null
        },
        logicalMatch.interval != null)
      .itemIf("subset",
        subsetToString(logicalMatch.subsets),
        !logicalMatch.subsets.isEmpty)
      .item("define", logicalMatch.patternDefinitions)
  }
}
