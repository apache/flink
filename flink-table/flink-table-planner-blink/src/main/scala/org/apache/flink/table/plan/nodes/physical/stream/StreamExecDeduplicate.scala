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

import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.EqualiserCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.generated.GeneratedRecordEqualiser
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.KeySelectorUtil
import org.apache.flink.table.runtime.bundle.KeyedMapBundleOperator
import org.apache.flink.table.runtime.bundle.trigger.CountBundleTrigger
import org.apache.flink.table.runtime.deduplicate.{DeduplicateFunction,
MiniBatchDeduplicateFunction}
import org.apache.flink.table.`type`.TypeConverters
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.typeutils.TypeCheckUtils.isRowTime

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.`type`.RelDataType

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode which deduplicate on keys and keeps only first row or last row.
  * This node is an optimization of [[StreamExecRank]] for some special cases.
  * Compared to [[StreamExecRank]], this node could use mini-batch and access less state.
  * <p>NOTES: only supports sort on proctime now.
  */
class StreamExecDeduplicate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    uniqueKeys: Array[Int],
    isRowtime: Boolean,
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
      isRowtime,
      keepLastRow)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val fieldNames = getRowType.getFieldNames
    val orderString = if (isRowtime) "ROWTIME" else "PROCTIME"
    super.explainTerms(pw)
      .item("keepLastRow", keepLastRow)
      .item("key", uniqueKeys.map(fieldNames.get).mkString(", "))
      .item("order", orderString)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(getInput)

    if (inputIsAccRetract) {
      throw new TableException(
        "Deduplicate: Retraction on Deduplicate is not supported yet.\n" +
          "please re-check sql grammar. \n" +
          "Note: Deduplicate should not follow a non-windowed GroupBy aggregation.")
    }

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)

    val inputRowType = FlinkTypeFactory.toInternalRowType(getInput.getRowType)
    val rowTimeFieldIndex = inputRowType.getFieldTypes.zipWithIndex
      .filter(e => isRowTime(e._1))
      .map(_._2)
    if (rowTimeFieldIndex.size > 1) {
      throw new RuntimeException("More than one row time field. Currently this is not supported!")
    }
    if (rowTimeFieldIndex.nonEmpty) {
      throw new TableException("Currently not support Deduplicate on rowtime.")
    }
    val tableConfig = tableEnv.getConfig
    val isMiniBatchEnabled = tableConfig.getConf.getLong(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY) > 0
    val generatedRecordEqualiser = generateRecordEqualiser(rowTypeInfo)
    val operator = if (isMiniBatchEnabled) {
      val exeConfig = tableEnv.execEnv.getConfig
      val processFunction = new MiniBatchDeduplicateFunction(
        rowTypeInfo,
        generateRetraction,
        rowTypeInfo.createSerializer(exeConfig),
        keepLastRow,
        generatedRecordEqualiser)
      val trigger = new CountBundleTrigger[BaseRow](
        tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_MINIBATCH_SIZE))
      new KeyedMapBundleOperator(
        processFunction,
        trigger)
    } else {
      val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
      val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
      val processFunction = new DeduplicateFunction(
        minRetentionTime,
        maxRetentionTime,
        rowTypeInfo,
        generateRetraction,
        keepLastRow,
        generatedRecordEqualiser)
      new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](processFunction)
    }
    val ret = new OneInputTransformation(
      inputTransform, getOperatorName, operator, rowTypeInfo, inputTransform.getParallelism)
    val selector = KeySelectorUtil.getBaseRowSelector(uniqueKeys, rowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  private def getOperatorName: String = {
    val fieldNames = getRowType.getFieldNames
    val keyNames = uniqueKeys.map(fieldNames.get).mkString(", ")
    val orderString = if (isRowtime) "ROWTIME" else "PROCTIME"
    s"${if (keepLastRow) "keepLastRow" else "KeepFirstRow"}" +
      s": (key: ($keyNames), select: (${fieldNames.mkString(", ")}), order: ($orderString))"
  }

  private def generateRecordEqualiser(rowTypeInfo: BaseRowTypeInfo): GeneratedRecordEqualiser = {
    val generator = new EqualiserCodeGenerator(
      rowTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo))
    val equaliserName = s"${if (keepLastRow) "LastRow" else "FirstRow"}ValueEqualiser"
    generator.generateRecordEqualiser(equaliserName)
  }

}
