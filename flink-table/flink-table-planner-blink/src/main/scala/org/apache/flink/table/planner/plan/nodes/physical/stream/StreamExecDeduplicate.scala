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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, KeySelectorUtil}
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator
import org.apache.flink.table.runtime.operators.deduplicate.{DeduplicateKeepFirstRowFunction, DeduplicateKeepLastRowFunction, MiniBatchDeduplicateKeepFirstRowFunction, MiniBatchDeduplicateKeepLastRowFunction}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode which deduplicate on keys and keeps only first row or last row.
  * This node is an optimization of [[StreamExecRank]] for some special cases.
  * Compared to [[StreamExecRank]], this node could use mini-batch and access less state.
  * <p>NOTES: only supports sort on proctime now, sort on rowtime will not translated into
  * StreamExecDeduplicate node.
  */
class StreamExecDeduplicate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    uniqueKeys: Array[Int],
    keepLastRow: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def producesUpdates: Boolean = keepLastRow

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = true

  override def consumesRetractions: Boolean = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecDeduplicate(
      cluster,
      traitSet,
      inputs.get(0),
      uniqueKeys,
      keepLastRow)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    val keep = if (keepLastRow) "LastRow" else "FirstRow"
    super.explainTerms(pw)
      .item("keep", keep)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .item("order", "PROCTIME")
  }

  //~ ExecNode methods -----------------------------------------------------------

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
    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(getInput)

    if (inputIsAccRetract) {
      throw new TableException("Deduplicate doesn't support retraction input stream currently.")
    }

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val tableConfig = planner.getTableConfig
    val isMiniBatchEnabled = tableConfig.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
    val operator = if (isMiniBatchEnabled) {
      val exeConfig = planner.getExecEnv.getConfig
      val rowSerializer = rowTypeInfo.createSerializer(exeConfig)
      val processFunction = if (keepLastRow) {
        new MiniBatchDeduplicateKeepLastRowFunction(rowTypeInfo, generateRetraction, rowSerializer)
      } else {
        new MiniBatchDeduplicateKeepFirstRowFunction(rowSerializer)
      }
      val trigger = AggregateUtil.createMiniBatchTrigger(tableConfig)
      new KeyedMapBundleOperator(
        processFunction,
        trigger)
    } else {
      val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
      val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
      val processFunction = if (keepLastRow) {
        new DeduplicateKeepLastRowFunction(minRetentionTime, maxRetentionTime, rowTypeInfo,
          generateRetraction)
      } else {
        new DeduplicateKeepFirstRowFunction(minRetentionTime, maxRetentionTime)
      }
      new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](processFunction)
    }
    val ret = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      rowTypeInfo,
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    val selector = KeySelectorUtil.getBaseRowSelector(uniqueKeys, rowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
