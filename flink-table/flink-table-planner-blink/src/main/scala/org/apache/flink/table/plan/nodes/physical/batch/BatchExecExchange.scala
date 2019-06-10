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

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.{PartitionTransformation, StreamTransformation}
import org.apache.flink.streaming.runtime.partitioner.{BroadcastPartitioner, GlobalPartitioner, RebalancePartitioner}
import org.apache.flink.table.api.{BatchTableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGeneratorContext, HashCodeGenerator}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonPhysicalExchange
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.runtime.BinaryHashPartitioner
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter}

import java.util

import scala.collection.JavaConversions._

/**
  * This RelNode represents a change of partitioning of the input elements.
  *
  * This does not create a physical transformation If its relDistribution' type is not range,
  * it only affects how upstream operations are connected to downstream operations.
  *
  * But if the type is range, this relNode will create some physical transformation because it
  * need calculate the data distribution. To calculate the data distribution, the received stream
  * will split in two process stream. For the first process stream, it will go through the sample
  * and statistics to calculate the data distribution in pipeline mode. For the second process
  * stream will been bocked. After the first process stream has been calculated successfully,
  * then the two process stream  will union together. Thus it can partitioner the record based
  * the data distribution. Then The RelNode will create the following transformations.
  *
  * +---------------------------------------------------------------------------------------------+
  * |                                                                                             |
  * | +-----------------------------+                                                             |
  * | | StreamTransformation        | ------------------------------------>                       |
  * | +-----------------------------+                                     |                       |
  * |                 |                                                   |                       |
  * |                 |                                                   |                       |
  * |                 |forward & PIPELINED                                |                       |
  * |                \|/                                                  |                       |
  * | +--------------------------------------------+                      |                       |
  * | | OneInputTransformation[LocalSample, n]     |                      |                       |
  * | +--------------------------------------------+                      |                       |
  * |                      |                                              |forward & BATCH        |
  * |                      |forward & PIPELINED                           |                       |
  * |                     \|/                                             |                       |
  * | +--------------------------------------------------+                |                       |
  * | |OneInputTransformation[SampleAndHistogram, 1]     |                |                       |
  * | +--------------------------------------------------+                |                       |
  * |                        |                                            |                       |
  * |                        |broadcast & PIPELINED                       |                       |
  * |                        |                                            |                       |
  * |                       \|/                                          \|/                      |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               TwoInputTransformation[AssignRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                       |                                                     |
  * |                                       |custom & PIPELINED                                   |
  * |                                      \|/                                                    |
  * | +---------------------------------------------------+------------------------------+        |
  * | |               OneInputTransformation[RemoveRangeId, n]                           |        |
  * | +----------------------------------------------------+-----------------------------+        |
  * |                                                                                             |
  * +---------------------------------------------------------------------------------------------+
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
  private var reusedInput: Option[StreamTransformation[BaseRow]] = None
  // the required exchange mode for reusable ExchangeBatchExec
  // if it's None, use value from getDataExchangeMode
  private var requiredExchangeMode: Option[DataExchangeMode] = None

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): BatchExecExchange = {
    new BatchExecExchange(cluster, traitSet, newInput, relDistribution)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("exchange_mode", requiredExchangeMode.orNull,
        requiredExchangeMode.contains(DataExchangeMode.BATCH))
  }

  //~ ExecNode methods -----------------------------------------------------------

  def setRequiredDataExchangeMode(exchangeMode: DataExchangeMode): Unit = {
    require(exchangeMode != null)
    requiredExchangeMode = Some(exchangeMode)
  }

  private[flink] def getDataExchangeMode(tableConf: Configuration): DataExchangeMode = {
    requiredExchangeMode match {
      case Some(mode) if mode eq DataExchangeMode.BATCH => mode
      case _ => DataExchangeMode.PIPELINED
    }
  }

  override def getDamBehavior: DamBehavior = {
    distribution.getType match {
      case RelDistribution.Type.RANGE_DISTRIBUTED => DamBehavior.FULL_DAM
      case _ => DamBehavior.PIPELINED
    }
  }

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]])

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {
    val input = reusedInput match {
      case Some(transformation) => transformation
      case None =>
        val input = getInputNodes.get(0).translateToPlan(tableEnv)
            .asInstanceOf[StreamTransformation[BaseRow]]
        reusedInput = Some(input)
        input
    }

    val inputType = input.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outputRowType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    // TODO supports DataExchangeMode.BATCH in runtime
    if (requiredExchangeMode.contains(DataExchangeMode.BATCH)) {
      throw new TableException("DataExchangeMode.BATCH is not supported now")
    }

    relDistribution.getType match {
      case RelDistribution.Type.ANY =>
        val transformation = new PartitionTransformation(
          input,
          null)
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.SINGLETON =>
        val transformation = new PartitionTransformation(
          input,
          new GlobalPartitioner[BaseRow])
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.RANDOM_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new RebalancePartitioner[BaseRow])
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.BROADCAST_DISTRIBUTED =>
        val transformation = new PartitionTransformation(
          input,
          new BroadcastPartitioner[BaseRow])
        transformation.setOutputType(outputRowType)
        transformation

      case RelDistribution.Type.HASH_DISTRIBUTED =>
        // TODO Eliminate duplicate keys
        val keys = relDistribution.getKeys
        val partitioner = new BinaryHashPartitioner(
          HashCodeGenerator.generateRowHash(
            CodeGeneratorContext(tableEnv.config),
            RowType.of(inputType.getLogicalTypes: _*),
            "HashPartitioner",
            keys.map(_.intValue()).toArray),
          keys.map(getInput.getRowType.getFieldNames.get(_)).toArray
        )
        val transformation = new PartitionTransformation(
          input,
          partitioner)
        transformation.setOutputType(outputRowType)
        transformation
      case _ =>
        throw new UnsupportedOperationException(
          s"not support RelDistribution: ${relDistribution.getType} now!")
    }
  }
}

