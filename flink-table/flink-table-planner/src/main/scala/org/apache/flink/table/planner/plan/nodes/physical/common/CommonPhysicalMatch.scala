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
package org.apache.flink.table.planner.plan.nodes.physical.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.logical.MatchRecognize
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import scala.collection.JavaConversions._

/** Base physical RelNode which matches along with MATCH_RECOGNIZE. */
abstract class CommonPhysicalMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val logicalMatch: MatchRecognize,
    outputRowType: RelDataType)
  extends SingleRel(cluster, traitSet, inputNode)
  with FlinkPhysicalRel {

  if (
    logicalMatch.measures.values().exists(containsPythonCall(_)) ||
    logicalMatch.patternDefinitions.values().exists(containsPythonCall(_))
  ) {
    throw new TableException("Python Function can not be used in MATCH_RECOGNIZE for now.")
  }

  override def deriveRowType(): RelDataType = outputRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val fieldNames = inputRowType.getFieldNames.toList
    super
      .explainTerms(pw)
      .itemIf(
        "partitionBy",
        fieldToString(logicalMatch.partitionKeys.toArray, inputRowType),
        !logicalMatch.partitionKeys.isEmpty)
      .itemIf(
        "orderBy",
        collationToString(logicalMatch.orderKeys, inputRowType),
        !logicalMatch.orderKeys.getFieldCollations.isEmpty)
      .itemIf(
        "measures",
        measuresDefineToString(
          logicalMatch.measures,
          fieldNames,
          getExpressionString,
          convertToExpressionDetail(pw.getDetailLevel)),
        !logicalMatch.measures.isEmpty
      )
      .item("rowsPerMatch", rowsPerMatchToString(logicalMatch.allRows))
      .item("after", afterMatchToString(logicalMatch.after, fieldNames))
      .item("pattern", logicalMatch.pattern.toString)
      .itemIf("subset", subsetToString(logicalMatch.subsets), !logicalMatch.subsets.isEmpty)
      .item("define", logicalMatch.patternDefinitions)
  }
}
