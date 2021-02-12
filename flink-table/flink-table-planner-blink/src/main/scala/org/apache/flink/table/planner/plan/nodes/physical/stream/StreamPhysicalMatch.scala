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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.logical.MatchRecognize
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch
import org.apache.flink.table.planner.plan.nodes.exec.{InputProperty, ExecNode}
import org.apache.flink.table.planner.plan.utils.MatchUtil
import org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall
import org.apache.flink.table.planner.plan.utils.RelExplainUtil._

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType

import _root_.java.util

import _root_.scala.collection.JavaConversions._

/**
 * Stream physical RelNode which matches along with MATCH_RECOGNIZE.
 */
class StreamPhysicalMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    val logicalMatch: MatchRecognize,
    outputRowType: RelDataType)
  extends SingleRel(cluster, traitSet, inputNode)
  with StreamPhysicalRel {

  if (logicalMatch.measures.values().exists(containsPythonCall(_)) ||
    logicalMatch.patternDefinitions.values().exists(containsPythonCall(_))) {
    throw new TableException("Python Function can not be used in MATCH_RECOGNIZE for now.")
  }

  override def requireWatermark: Boolean = {
    val rowtimeFields = getInput.getRowType.getFieldList
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    rowtimeFields.nonEmpty
  }

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalMatch(
      cluster,
      traitSet,
      inputs.get(0),
      logicalMatch,
      outputRowType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    val fieldNames = inputRowType.getFieldNames.toList
    super.explainTerms(pw)
      .itemIf("partitionBy",
        fieldToString(logicalMatch.partitionKeys.toArray, inputRowType),
        !logicalMatch.partitionKeys.isEmpty)
      .itemIf("orderBy",
        collationToString(logicalMatch.orderKeys, inputRowType),
        !logicalMatch.orderKeys.getFieldCollations.isEmpty)
      .itemIf("measures",
        measuresDefineToString(logicalMatch.measures, fieldNames, getExpressionString),
        !logicalMatch.measures.isEmpty)
      .item("rowsPerMatch", rowsPerMatchToString(logicalMatch.allRows))
      .item("after", afterMatchToString(logicalMatch.after, fieldNames))
      .item("pattern", logicalMatch.pattern.toString)
      .itemIf("subset",
        subsetToString(logicalMatch.subsets),
        !logicalMatch.subsets.isEmpty)
      .item("define", logicalMatch.patternDefinitions)
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecMatch(
      MatchUtil.createMatchSpec(logicalMatch),
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }
}
