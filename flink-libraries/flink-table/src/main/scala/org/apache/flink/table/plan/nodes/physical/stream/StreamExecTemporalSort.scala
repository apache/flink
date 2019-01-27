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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.types.TypeConverters
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.errorcode.TableErrors
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.sort.{OnlyRowTimeSortOperator, ProcTimeSortOperator, RowTimeSortOperator}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import _root_.scala.collection.JavaConverters._

/**
 * Flink RelNode which matches along with Sort Rule.
 */
class StreamExecTemporalSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    inputSchema: BaseRowSchema,
    outputSchema: BaseRowSchema,
    sortCollation: RelCollation,
    description: String)
  extends Sort(cluster, traitSet, inputNode, sortCollation)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamExecTemporalSort(
      cluster,
      traitSet,
      input,
      inputSchema,
      outputSchema,
      newCollation,
      description)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    SortUtil.sortExplainTerms(
      pw.input("input", getInput()),
      outputSchema.relDataType,
      sortCollation,
      null,
      null)
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    // need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getFirstSortField(sortCollation, outputSchema.relDataType).getType

    // time ordering needs to be ascending
    if (SortUtil.getFirstSortDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException(
        TableErrors.INST.sqlSortOrderError())}

    val managedMemory = NodeResourceUtil.getSortBufferManagedMemory(tableEnv.getConfig.getConf)
    val mangedMemorySize = managedMemory * NodeResourceUtil.SIZE_IN_MB

    // enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
      case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
        createSortProcTime(inputTransformation, mangedMemorySize)
      case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        createSortRowTime(inputTransformation, mangedMemorySize)
      case _ =>
        throw new TableException(TableErrors.INST.sqlSortInternalError())
    }
  }

  /**
    * Create Sort logic based on processing time
    */
  def createSortProcTime(
      input: StreamTransformation[BaseRow],
      memorySize: Double): StreamTransformation[BaseRow] = {

    val returnTypeInfo = outputSchema.typeInfo()
      .asInstanceOf[BaseRowTypeInfo]
    val inputTypeInfo = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    // if the order has secondary sorting fields in addition to the proctime
    if (sortCollation.getFieldCollations.size() > 1) {

      // strip off time collation
      val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(
        sortCollation.getFieldCollations.asScala.tail)

      val generatedSorter = SorterHelper.createSorter(
        inputTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
        sortFields,
        sortDirections,
        nullsIsLast)

      val sortOperator = new ProcTimeSortOperator(
        inputTypeInfo,
        generatedSorter,
        memorySize)
      val ret = new OneInputTransformation(
        input, "ProcTimeSortOperator", sortOperator, returnTypeInfo, 1)
      val selector = new NullBinaryRowKeySelector
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
  def createSortRowTime(
      input: StreamTransformation[BaseRow],
      memorySize: Double): StreamTransformation[BaseRow] = {
    val rowtimeIdx = sortCollation.getFieldCollations.get(0).getFieldIndex
    val returnTypeInfo = outputSchema.typeInfo()
      .asInstanceOf[BaseRowTypeInfo]
    val inputTypeInfo = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val sortOperator = if (sortCollation.getFieldCollations.size() > 1) {
      // strip off time collation
      val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(
        sortCollation.getFieldCollations.asScala.tail)
      val generatedSorter = SorterHelper.createSorter(
        inputTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
        sortFields,
        sortDirections,
        nullsIsLast)

      new RowTimeSortOperator(
        inputTypeInfo,
        generatedSorter,
        rowtimeIdx,
        memorySize)
    } else {
      new OnlyRowTimeSortOperator(inputTypeInfo, rowtimeIdx)
    }
    val ret = new OneInputTransformation(
      input, "RowTimeSortOperator", sortOperator, returnTypeInfo, 1)
    val selector = new NullBinaryRowKeySelector
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)

    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    ret
  }
}
