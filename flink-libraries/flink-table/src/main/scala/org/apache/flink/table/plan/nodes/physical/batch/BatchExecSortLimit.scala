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
package org.apache.flink.table.plan.nodes.physical.batch

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{BatchTableEnvironment, TableException}
import org.apache.flink.table.codegen.{GeneratedSorter, SortCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.cost.FlinkBatchCost._
import org.apache.flink.table.plan.cost.FlinkCostFactory
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.sort.SortLimitOperator
import org.apache.flink.table.typeutils._

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode, RelWriter}
import org.apache.calcite.rex.{RexLiteral, RexNode}

import _root_.scala.collection.JavaConverters._

/**
  * This RelNode take the `limit` elements beginning with the first `offset` elements.
  *
  * Firstly it take the first `offset + limit` elements of each child partition, secondly the child
  * partition will forward elements to a single partition, lastly it take the `limit` elements
  * beginning with the first `offset` elements from the single output partition.
  **/
class BatchExecSortLimit(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inp: RelNode,
    collations: RelCollation,
    sortOffset: RexNode,
    limit: RexNode,
    isGlobal: Boolean,
    ruleDescription: String)
  extends Sort(cluster, traitSet, inp, collations, sortOffset, limit)
  with BatchPhysicalRel
  with RowBatchExecNode {

  private val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
    collations.getFieldCollations.asScala)

  private val limitStart: Long =  SortUtil.getFetchLimitStart(offset)
  private val limitEnd: Long = SortUtil.getFetchLimitEnd(limit, offset)

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new BatchExecSortLimit(
      cluster,
      traitSet,
      newInput,
      newCollation,
      offset,
      fetch,
      isGlobal,
      ruleDescription)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("orderBy", SortUtil.sortFieldsToString(collations, getRowType))
      .item("offset", offsetToString)
      .item("limit", limitToString)
      .item("global", isGlobal)
  }

  override def isDeterministic: Boolean = SortUtil.isDeterministic(offset, fetch)

  override def estimateRowCount(metadata: RelMetadataQuery): Double = {
    val inputRowCnt = metadata.getRowCount(this.getInput)
    if (inputRowCnt == null) {
      inputRowCnt
    } else {
      val rowCount = (inputRowCnt - limitStart).max(1.0)
      if (limit != null) {
        rowCount.min(RexLiteral.intValue(limit))
      } else {
        rowCount
      }
    }
  }

  private def offsetToString: String = {
    val offsetValue = if (offset != null) {
      RexLiteral.intValue(offset)
    } else {
      0
    }
    s"$offsetValue"
  }

  private def limitToString: String = {
    if (limit != null) {
      s"${RexLiteral.intValue(limit)}"
    } else {
      "unlimited"
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCount = mq.getRowCount(getInput())
    val heapLen = Math.min(rowCount, limitEnd)
    val numOfSort = collations.getFieldCollations.size()
    val cpuCost = COMPARE_CPU_COST * numOfSort * rowCount * Math.log(heapLen)
    // assume memory is big enough to simplify the estimation.
    val memCost = heapLen * mq.getAverageRowSize(this)
    val costFactory = planner.getCostFactory.asInstanceOf[FlinkCostFactory]
    costFactory.makeCost(mq.getRowCount(this), cpuCost, 0, 0, memCost)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.FULL_DAM

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    if (limitEnd == Long.MaxValue) {
      throw new TableException("Not support limitEnd is max value now!")
    }

    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val types = inputType.getFieldTypes
    val binaryType = new BaseRowTypeInfo(types: _*)

    // generate comparator
    val (comparators, serializers) = TypeUtils.flattenComparatorAndSerializer(
      binaryType.getArity, keys, orders, types)
    val generator = new SortCodeGenerator(
      keys, keys.map((key) => types(key)).map(TypeConverters.createInternalTypeFromTypeInfo),
      comparators, orders, nullsIsLast)

    // TODO If input is ordered, there is no need to use the heap.
    val operator = new SortLimitOperator(
      isGlobal,
      limitStart,
      limitEnd,
      GeneratedSorter(
        generator.generateNormalizedKeyComputer("SortLimitComputer"),
        generator.generateRecordComparator("SortLimitComparator"),
        serializers, comparators))

    val transformation = new OneInputTransformation(
      input,
      getOperatorName,
      operator,
      inputType,
      getResource.getParallelism)
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setDamBehavior(getDamBehavior)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

  private def getOperatorName = {
    s"${if (isGlobal) "Global" else "Local"}SortLimit(" +
        s"orderBy: [${SortUtil.sortFieldsToString(collations, getRowType)}], " +
        s"offset: $offsetToString, " +
        s"limit: $limitToString)"
  }

}
