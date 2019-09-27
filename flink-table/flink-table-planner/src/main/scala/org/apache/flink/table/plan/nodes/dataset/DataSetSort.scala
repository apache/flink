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

package org.apache.flink.table.plan.nodes.dataset

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexLiteral, RexNode}
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.internal.BatchTableEnvImpl
import org.apache.flink.table.api.{BatchQueryConfig, TableException}
import org.apache.flink.table.plan.nodes.CommonSort
import org.apache.flink.table.runtime.aggregate.SortUtil.directionToOrder
import org.apache.flink.table.runtime.{CountPartitionFunction, LimitFilterFunction}
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class DataSetSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    collations: RelCollation,
    rowRelDataType: RelDataType,
    offset: RexNode,
    fetch: RexNode)
  extends SingleRel(cluster, traitSet, inp)
  with CommonSort
  with DataSetRel {

  private val limitStart: Long =  getFetchLimitStart(offset)

  private val limitEnd: Long = getFetchLimitEnd(fetch, offset)

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataSetSort(
      cluster,
      traitSet,
      inputs.get(0),
      collations,
      getRowType,
      offset,
      fetch
    )
  }

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val inputRowCnt = metadata.getRowCount(this.getInput)
    if (inputRowCnt == null) {
      inputRowCnt
    } else {
      val rowCount = (inputRowCnt - limitStart).max(1.0)
      if (fetch != null) {
        val limit = RexLiteral.intValue(fetch)
        rowCount.min(limit)
      } else {
        rowCount
      }
    }
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvImpl,
      queryConfig: BatchQueryConfig): DataSet[Row] = {

    if (fieldCollations.isEmpty) {
      throw new TableException("Limiting the result without sorting is not allowed " +
        "as it could lead to arbitrary results.")
    }

    val config = tableEnv.getConfig

    val inputDs = inp.asInstanceOf[DataSetRel].translateToPlan(tableEnv, queryConfig)

    val currentParallelism = inputDs.getExecutionEnvironment.getParallelism
    var partitionedDs = if (currentParallelism == 1) {
      inputDs
    } else {
      inputDs.partitionByRange(fieldCollations.map(_._1): _*)
        .withOrders(fieldCollations.map(_._2): _*)
    }

    fieldCollations.foreach { fieldCollation =>
      partitionedDs = partitionedDs.sortPartition(fieldCollation._1, fieldCollation._2)
    }

    if (offset == null && fetch == null) {
      partitionedDs
    } else {
      val countFunction = new CountPartitionFunction[Row]

      val partitionCountName = s"prepare offset/fetch"

      val partitionCount = partitionedDs
        .mapPartition(countFunction)
        .name(partitionCountName)

      val broadcastName = "countPartition"

      val limitFunction = new LimitFilterFunction[Row](
        limitStart,
        limitEnd,
        broadcastName)

      val limitName = s"offset: $$offsetToString(offset), fetch: $$fetchToString(fetch, offset))"

      partitionedDs
        .filter(limitFunction)
        .name(limitName)
        .withBroadcastSet(partitionCount, broadcastName)
    }
  }

  private val fieldCollations = collations.getFieldCollations.asScala
    .map(c => (c.getFieldIndex, directionToOrder(c.getDirection)))

  override def toString: String = {
    sortToString(getRowType, collations, offset, fetch)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    sortExplainTerms(
      super.explainTerms(pw),
      getRowType,
      collations,
      offset,
      fetch)
  }
}
