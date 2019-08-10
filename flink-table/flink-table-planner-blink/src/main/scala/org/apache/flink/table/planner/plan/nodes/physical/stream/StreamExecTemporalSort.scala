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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.operators.sort.{ProcTimeSortOperator, RowTimeSortOperator}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for time-ascending-order [[Sort]] without `limit`.
  *
  * @see [[StreamExecRank]] which must be with `limit` order by.
  * @see [[StreamExecSort]] which can be used for testing now, its sort key can be any type.
  */
class StreamExecTemporalSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation)
  extends Sort(cluster, traitSet, inputRel, sortCollation)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamExecTemporalSort(cluster, traitSet, input, newCollation)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput())
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * Returns an array of this node's inputs. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return Array of this node's inputs
    */
  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig
    // time ordering needs to be ascending
    if (SortUtil.getFirstSortDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException(
        "Sort: Primary sort order of a streaming table must be ascending on time.\n" +
          "please re-check sort statement according to the description above")
    }

    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val timeType = SortUtil.getFirstSortField(sortCollation, getRowType).getType
    timeType match {
      case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
        createSortProcTime(input, config)
      case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        createSortRowTime(input, config)
      case _ =>
        throw new TableException(
          "Sort: Internal Error\n" +
            "Normally, this happens unlikely. please contact customer support for this"
        )
    }
  }

  /**
    * Create Sort logic based on processing time
    */
  private def createSortProcTime(
      input: Transformation[BaseRow],
      tableConfig: TableConfig): Transformation[BaseRow] = {
    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val fieldCollations = sortCollation.getFieldCollations
    // if the order has secondary sorting fields in addition to the proctime
    if (fieldCollations.size() > 1) {
      // strip off time collation
      val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations.tail)
      // sort code gen
      val keyTypes = keys.map(inputType.getTypeAt)
      val rowComparator = ComparatorCodeGenerator.gen(tableConfig, "ProcTimeSortComparator",
        keys, keyTypes, orders, nullsIsLast)
      val sortOperator = new ProcTimeSortOperator(BaseRowTypeInfo.of(inputType), rowComparator)
      val outputRowTypeInfo = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

      // as input node is singleton exchange, its parallelism is 1.
      val ret = new OneInputTransformation(
        input,
        getRelDetailedDescription,
        sortOperator,
        outputRowTypeInfo,
        input.getParallelism)

      val selector = NullBinaryRowKeySelector.INSTANCE
      ret.setStateKeySelector(selector)
      ret.setStateKeyType(selector.getProducedType)
      ret
    } else {
      // if the order is done only on proctime we only need to forward the elements
      input
    }
  }

  /**
    * Create Sort logic based on row time
    */
  private def createSortRowTime(
      input: Transformation[BaseRow],
      tableConfig: TableConfig): Transformation[BaseRow] = {
    val fieldCollations = sortCollation.getFieldCollations
    val rowTimeIdx = fieldCollations.get(0).getFieldIndex
    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val rowComparator = if (fieldCollations.size() > 1) {
      // strip off time collation
      val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations.tail)
      // comparator code gen
      val keyTypes = keys.map(inputType.getTypeAt)
      ComparatorCodeGenerator.gen(tableConfig, "RowTimeSortComparator", keys, keyTypes, orders,
        nullsIsLast)
    } else {
      null
    }
    val sortOperator = new RowTimeSortOperator(
      BaseRowTypeInfo.of(inputType), rowTimeIdx, rowComparator)
    val outputRowTypeInfo = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val ret = new OneInputTransformation(
      input,
      getRelDetailedDescription,
      sortOperator,
      outputRowTypeInfo,
      input.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    val selector = NullBinaryRowKeySelector.INSTANCE
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
