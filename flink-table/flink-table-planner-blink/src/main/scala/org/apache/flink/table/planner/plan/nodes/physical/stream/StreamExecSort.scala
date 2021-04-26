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
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.StreamExecNode
import org.apache.flink.table.planner.plan.utils.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.operators.sort.StreamSortOperator
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import java.lang.{Boolean => JBoolean}

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
  with StreamExecNode[RowData] {

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

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    if (!config.getConfiguration.getBoolean(StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED)) {
      throw new TableException("Sort on a non-time-attribute field is not supported.")
    }

    val inputType = FlinkTypeFactory.toLogicalRowType(getInput.getRowType)
    val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(sortCollation.getFieldCollations)
    // sort code gen
    val keyTypes = keys.map(inputType.getTypeAt)
    val rowComparator = ComparatorCodeGenerator.gen(config, "StreamExecSortComparator",
      keys, keyTypes, orders, nullsIsLast)
    val sortOperator = new StreamSortOperator(InternalTypeInfo.of(inputType), rowComparator)
    val input = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val outputRowTypeInfo = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    // as input node is singleton exchange, its parallelism is 1.
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
    ret
  }
}
object StreamExecSort {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED: ConfigOption[JBoolean] =
  key("table.exec.non-temporal-sort.enabled")
      .defaultValue(JBoolean.valueOf(false))
      .withDescription("Set whether to enable universal sort for stream. When it is false, " +
          "universal sort can't use for stream, default false. Just for testing.")
}
