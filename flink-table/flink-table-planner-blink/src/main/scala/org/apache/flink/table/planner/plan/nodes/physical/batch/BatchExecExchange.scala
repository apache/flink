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

package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{PartitionTransformation, ShuffleMode}
import org.apache.flink.streaming.runtime.partitioner.{BroadcastPartitioner, GlobalPartitioner, RebalancePartitioner}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, HashCodeGenerator}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalExchange
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.runtime.partitioner.BinaryHashPartitioner
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * This RelNode represents a change of partitioning of the input elements.
  *
  * This does not create a physical transformation if its relDistribution' type is not range which
  * is not supported now.
  */
class BatchExecExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, inputRel, relDistribution)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  // TODO reuse PartitionTransformation
  // currently, an Exchange' input transformation will be reused if it is reusable,
  // and different PartitionTransformation objects will be created which have same input.
  // cache input transformation to reuse
  private var reusedInput: Option[Transformation[BaseRow]] = None
  // the required shuffle mode for reusable ExchangeBatchExec
  // if it's None, use value from getShuffleMode
  private var requiredShuffleMode: Option[ShuffleMode] = None

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchExecExchange = {
    new BatchExecExchange(cluster, traitSet, newInput, relDistribution)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("shuffle_mode", requiredShuffleMode.orNull,
        requiredShuffleMode.contains(ShuffleMode.BATCH))
  }

  //~ ExecNode methods -----------------------------------------------------------

  def setRequiredShuffleMode(shuffleMode: ShuffleMode): Unit = {
    require(shuffleMode != null)
    requiredShuffleMode = Some(shuffleMode)
  }

  private[flink] def getShuffleMode(tableConf: Configuration): ShuffleMode = {
    requiredShuffleMode match {
      case Some(mode) if mode eq ShuffleMode.BATCH => mode
      case _ =>
        if (tableConf.getString(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE)
            .equalsIgnoreCase(ShuffleMode.BATCH.toString)) {
          ShuffleMode.BATCH
        } else {
          ShuffleMode.UNDEFINED
        }
    }
  }

  override def getDamBehavior: DamBehavior = {
    val tableConfig = FlinkRelOptUtil.getTableConfigFromContext(this)
    val shuffleMode = getShuffleMode(tableConfig.getConfiguration)
    if (shuffleMode eq ShuffleMode.BATCH) {
      return DamBehavior.FULL_DAM
    }
    distribution.getType match {
      case RelDistribution.Type.RANGE_DISTRIBUTED => DamBehavior.FULL_DAM
      case _ => DamBehavior.PIPELINED
    }
  }

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchPlanner, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[BaseRow] = {
    val input = reusedInput match {
      case Some(transformation) => transformation
      case None =>
        val input = getInputNodes.get(0).translateToPlan(planner)
            .asInstanceOf[Transformation[BaseRow]]
        reusedInput = Some(input)
        input
    }

    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outputRowType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val conf = planner.getTableConfig
    val shuffleMode = getShuffleMode(conf.getConfiguration)

    relDistribution.getType match {
      case RelDistribution.Type.ANY =>
        val transformation = new PartitionTransformation(
          input,
          null,
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
        transformation

      case RelDistribution.Type.SINGLETON =>
        val transformation = new PartitionTransformation(
          input,
          new GlobalPartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation.setParallelism(1)
        transformation

      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new RebalancePartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
        transformation

      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new BroadcastPartitioner[BaseRow],
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
        transformation

      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys
        val keys = relDistribution.getKeys
        val partitioner = new BinaryHashPartitioner(
          HashCodeGenerator.generateRowHash(
            CodeGeneratorContext(planner.getTableConfig),
            RowType.of(inputType.getLogicalTypes: _*),
            "HashPartitioner",
            keys.map(_.intValue()).toArray),
          keys.map(getInput.getRowType.getFieldNames.get(_)).toArray
        )
        val transformation = new PartitionTransformation(
          input,
          partitioner,
          shuffleMode)
        transformation.setOutputType(outputRowType)
        transformation.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT)
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }
}

