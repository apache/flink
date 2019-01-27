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
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.SortUtil
import org.apache.flink.table.runtime.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.runtime.sort.StreamSortOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.NodeResourceUtil

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import _root_.scala.collection.JavaConverters._

/**
  * Flink RelNode which matches along with Sort Rule.<br/>
  * In [[StreamExecTemporalSort]], the first key must be TimeIndicatorType.<br/>
  * In [[StreamExecSort]], the key can be any type.<br/>
  * In [[StreamExecRank]], the order by must with limit.<br/>
  * For example:<br/>
  * ''SELECT * FROM A ORDER BY ROWTIME, a'' is in [[StreamExecTemporalSort]].<br/>
  * ''SELECT * FROM A ORDER BY a, ROWTIME'' is in [[StreamExecSort]].<br/>
  * ''SELECT * FROM A ORDER BY a LIMIT 2'' is in [[StreamExecRank]].<br/>
  *
  * This class is used for testing.
  * For simplicity, this class is used to represent the BatchExecSort,
  * BatchExecSortLimit, BatchExecLimit corresponding to the batch.
  */
class StreamExecSort(
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
    new StreamExecSort(
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
      offset,
      fetch)
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    if (!tableEnv.getConfig.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED)) {
      throw new TableException("Sort on a non-time-attribute field is not supported.")
    }

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val mangedMemorySize =
      NodeResourceUtil.getPerRequestManagedMemory(
        tableEnv.getConfig.getConf)* NodeResourceUtil.SIZE_IN_MB

    createSort(inputTransformation, mangedMemorySize)
  }

  /**
    * Create Sort logic
    */
  def createSort(
    input: StreamTransformation[BaseRow],
    memorySize: Double): StreamTransformation[BaseRow] = {
    val returnTypeInfo = outputSchema.typeInfo()
      .asInstanceOf[BaseRowTypeInfo]
    val inputTypeInfo = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val sortOperator = {
      val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(
        sortCollation.getFieldCollations.asScala)
      val generatedSorter = SorterHelper.createSorter(
        inputTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
        sortFields,
        sortDirections,
        nullsIsLast)

      new StreamSortOperator(
        inputTypeInfo,
        generatedSorter,
        memorySize)
    }
    val ret = new OneInputTransformation(
      input, "SortOperator", sortOperator, returnTypeInfo, 1)
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    val selector = new NullBinaryRowKeySelector
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
