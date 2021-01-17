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

import org.apache.flink.annotation.Experimental
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, SortUtil}
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSort

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Sort]].
  *
  * <b>NOTES:</b> This class is used for testing with bounded source now.
  * If a query is converted to this node in product environment, an exception will be thrown.
  *
  * @see [[StreamPhysicalTemporalSort]] which must be time-ascending-order sort without `limit`.
  *
  * e.g.
  *      ''SELECT * FROM TABLE ORDER BY ROWTIME, a'' will be converted to
  *         [[StreamPhysicalTemporalSort]]
  *      ''SELECT * FROM TABLE ORDER BY a, ROWTIME'' will be converted to [[StreamPhysicalSort]]
  */
@Experimental
class StreamPhysicalSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation)
  extends Sort(cluster, traitSet, inputRel, sortCollation)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamPhysicalSort(cluster, traitSet, input, newCollation)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput())
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecSort(
      SortUtil.getSortSpec(sortCollation.getFieldCollations),
      ExecEdge.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription
    )
  }

}
