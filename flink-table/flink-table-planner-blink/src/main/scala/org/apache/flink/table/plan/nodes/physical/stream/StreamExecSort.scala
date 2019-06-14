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

import org.apache.flink.annotation.Experimental
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.sort.StreamSortOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for [[Sort]].
  *
  * <b>NOTES:</b> This class is used for testing with bounded source now.
  * If a query is converted to this node in product environment, an exception will be thrown.
  *
  * @see [[StreamExecTemporalSort]] which must be time-ascending-order sort without `limit`.
  *
  * e.g.
  * ''SELECT * FROM TABLE ORDER BY ROWTIME, a'' will be converted to [[StreamExecTemporalSort]]
  * ''SELECT * FROM TABLE ORDER BY a, ROWTIME'' will be converted to [[StreamExecSort]]
  */
@Experimental
class StreamExecSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation)
  extends Sort(cluster, traitSet, inputRel, sortCollation)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  /**
    * this node will not produce or consume retraction message
    * due to it starts sending data to output after all input data has come.
    */
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
    new StreamExecSort(cluster, traitSet, input, newCollation)
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
  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  /**
    * Internal method, translates this node into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    */
  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val conf = tableEnv.getConfig
    if (!conf.getConf.getBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED)) {
      throw new TableException("Sort on a non-time-attribute field is not supported.")
    }

    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(sortCollation.getFieldCollations)
    // sort code gen
    val keyTypes = keys.map(inputType.getTypeAt)
    val rowComparator = ComparatorCodeGenerator.gen(conf, "StreamExecSortComparator",
      keys, keyTypes, orders, nullsIsLast)
    val sortOperator = new StreamSortOperator(BaseRowTypeInfo.of(inputType), rowComparator)
    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val outputRowTypeInfo = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    // as input node is singleton exchange, its parallelism is 1.
    val ret = new OneInputTransformation(
      input,
      s"Sort(${RelExplainUtil.collationToString(sortCollation, getRowType)})",
      sortOperator,
      outputRowTypeInfo,
      getResource.getParallelism)
    if (getResource.getMaxParallelism > 0) {
      ret.setMaxParallelism(getResource.getMaxParallelism)
    }
    ret
  }

}
