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

package org.apache.flink.table.plan.nodes.logical

import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.calcite.LogicalLastRow

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

import scala.collection.JavaConversions._

class FlinkLogicalLastRow(
   cluster: RelOptCluster,
   traitSet: RelTraitSet,
   input: RelNode,
   uniqueKeys: Array[Int],
   rowType: RelDataType)
  extends SingleRel(cluster, traitSet, input)
  with FlinkLogicalRel {

  override def deriveRowType(): RelDataType = rowType

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new FlinkLogicalLastRow(cluster, traitSet, inputs.get(0), uniqueKeys, rowType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val keyNames = uniqueKeys.map(input.getRowType.getFieldNames.get(_)).mkString(", ")
    super.explainTerms(pw)
      .item("key", keyNames)
      .item("select", rowType.getFieldNames.mkString(", "))
  }

  override def isDeterministic: Boolean = true
}

class FlinkLogicalLastRowConverter extends ConverterRule(
  classOf[LogicalLastRow],
  Convention.NONE,
  FlinkConventions.LOGICAL,
  "FlinkLogicalLastRowConverter") {

  override def convert(rel: RelNode): RelNode = {

    val lastRowNode = rel.asInstanceOf[LogicalLastRow]
    val traitSet = FlinkRelMetadataQuery.traitSet(rel).replace(FlinkConventions.LOGICAL).simplify()
    val newInput = RelOptRule.convert(lastRowNode.getInput, FlinkConventions.LOGICAL)
    val uniqueKeyNames = lastRowNode.uniqueKeys
    val uniqueKeyIndex = lastRowNode.getRowType.getFieldNames.zipWithIndex
      .filter(e => uniqueKeyNames.contains(e._1))
      .map(_._2).toArray
    new FlinkLogicalLastRow(
      rel.getCluster, traitSet, newInput, uniqueKeyIndex, rel.getRowType)
  }
}

object FlinkLogicalLastRow {
  val CONVERTER = new FlinkLogicalLastRowConverter
}
