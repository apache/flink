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
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.streaming.api.operators.{KeyedProcessOperator, OneInputStreamOperator}
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecDeduplicate.TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, KeySelectorUtil}
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger
import org.apache.flink.table.runtime.operators.deduplicate._
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.lang.{Boolean => JBoolean}
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
    val isRowtime: Boolean,
    val keepLastRow: Boolean)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with LegacyStreamExecNode[RowData] {

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = getInput.getRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecDeduplicate(
      cluster,
      traitSet,
      inputs.get(0),
      uniqueKeys,
      isRowtime,
      keepLastRow)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    val orderString = if (isRowtime) "ROWTIME" else "PROCTIME"
    val keep = if (keepLastRow) "LastRow" else "FirstRow"
    super.explainTerms(pw)
      .item("keep", keep)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .item("order", orderString)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[InternalTypeInfo[RowData]]
    val rowSerializer = rowTypeInfo.createSerializer(planner.getExecEnv.getConfig)
    val tableConfig = planner.getTableConfig

    val operator = if (isRowtime) {
      new RowtimeDeduplicateOperatorTranslator(rowTypeInfo, rowSerializer, tableConfig, this)
        .createDeduplicateOperator()
    } else {
      new ProcTimeDeduplicateOperatorTranslator(rowTypeInfo, rowSerializer, tableConfig, this)
        .createDeduplicateOperator()
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

    val selector = KeySelectorUtil.getRowDataSelector(uniqueKeys, rowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}

/**
 * Base translator to create deduplicate operator.
 */
abstract class DeduplicateOperatorTranslator(
    rowTypeInfo: InternalTypeInfo[RowData],
    serializer: TypeSerializer[RowData],
    tableConfig: TableConfig,
    deduplicate: StreamExecDeduplicate) {

  protected val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(deduplicate)
  protected val generateInsert = tableConfig.getConfiguration
    .getBoolean(TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE)
  protected val isMiniBatchEnabled = tableConfig.getConfiguration.getBoolean(
    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
  protected val minRetentionTime = tableConfig.getMinIdleStateRetentionTime

  protected val miniBatchSize = if (isMiniBatchEnabled) {
    val size = tableConfig.getConfiguration.getLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE)
    Preconditions.checkArgument(size > 0)
    size
  } else {
    -1L
  }

  protected val keepLastRow = deduplicate.keepLastRow

  def createDeduplicateOperator(): OneInputStreamOperator[RowData, RowData]

}

/**
 * Translator to create process time deduplicate operator.
 */
class RowtimeDeduplicateOperatorTranslator(
    rowTypeInfo: InternalTypeInfo[RowData],
    serializer: TypeSerializer[RowData],
    tableConfig: TableConfig,
    deduplicate: StreamExecDeduplicate)
  extends DeduplicateOperatorTranslator(
    rowTypeInfo,
    serializer,
    tableConfig,
    deduplicate) {

  override def createDeduplicateOperator(): OneInputStreamOperator[RowData, RowData] = {
    val rowtimeField = deduplicate.getInput.getRowType.getFieldList
      .filter(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    Preconditions.checkArgument(rowtimeField.nonEmpty)
    val rowtimeIndex = rowtimeField.get(0).getIndex
    if (isMiniBatchEnabled) {
      val trigger = new CountBundleTrigger[RowData](miniBatchSize)
      val processFunction = new RowTimeMiniBatchDeduplicateFunction(
        rowTypeInfo,
        serializer,
        minRetentionTime,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow)
      new KeyedMapBundleOperator(processFunction, trigger)
    } else {
      val processFunction = new RowTimeDeduplicateFunction(
        rowTypeInfo,
        minRetentionTime,
        rowtimeIndex,
        generateUpdateBefore,
        generateInsert,
        keepLastRow)
      new KeyedProcessOperator[RowData, RowData, RowData](processFunction)
    }
  }
}

/**
 * Translator to create process time deduplicate operator.
 */
class ProcTimeDeduplicateOperatorTranslator(
    rowTypeInfo: InternalTypeInfo[RowData],
    serializer: TypeSerializer[RowData],
    tableConfig: TableConfig,
    deduplicate: StreamExecDeduplicate)
  extends DeduplicateOperatorTranslator(
    rowTypeInfo,
    serializer,
    tableConfig,
    deduplicate) {

  override def createDeduplicateOperator(): OneInputStreamOperator[RowData, RowData] = {
    if (isMiniBatchEnabled) {
      val trigger = new CountBundleTrigger[RowData](miniBatchSize)
      if (keepLastRow) {
        val processFunction = new ProcTimeMiniBatchDeduplicateKeepLastRowFunction(
          rowTypeInfo,
          serializer,
          minRetentionTime,
          generateUpdateBefore,
          generateInsert,
          true)
        new KeyedMapBundleOperator(processFunction, trigger)
      } else {
        val processFunction = new ProcTimeMiniBatchDeduplicateKeepFirstRowFunction(
          serializer,
          minRetentionTime)
        new KeyedMapBundleOperator(processFunction, trigger)
      }
    } else {
      if (keepLastRow) {
        val processFunction = new ProcTimeDeduplicateKeepLastRowFunction(
          rowTypeInfo,
          minRetentionTime,
          generateUpdateBefore,
          generateInsert,
          true)
        new KeyedProcessOperator[RowData, RowData, RowData](processFunction)
      } else {
        val processFunction = new ProcTimeDeduplicateKeepFirstRowFunction(minRetentionTime)
        new KeyedProcessOperator[RowData, RowData, RowData](processFunction)
      }
    }
  }
}

object StreamExecDeduplicate {

  @Experimental
  val TABLE_EXEC_INSERT_AND_UPDATE_AFTER_SENSITIVE: ConfigOption[JBoolean] =
    key("table.exec.insert-and-updateafter-sensitive")
      .booleanType()
      .defaultValue(JBoolean.valueOf(true))
      .withDescription("Set whether the job (especially the sinks) is sensitive to " +
        "INSERT messages and UPDATE_AFTER messages. " +
        "If false, Flink may send UPDATE_AFTER instead of INSERT for the first row " +
        "at some times (e.g. deduplication for last row). " +
        "If true, Flink will guarantee to send INSERT for the first row, " +
        "but there will be additional overhead." +
        "Default is true.")
}
