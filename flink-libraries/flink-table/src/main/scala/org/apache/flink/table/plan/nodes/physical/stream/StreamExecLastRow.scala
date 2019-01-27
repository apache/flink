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
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.{AggregateUtil, StreamExecUtil}
import org.apache.flink.table.runtime.KeyedProcessOperator
import org.apache.flink.table.runtime.aggregate.{LastRowFunction, MiniBatchLastRowFunction}
import org.apache.flink.table.runtime.bundle.KeyedBundleOperator
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import java.util

import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with LogicalLastRow.
  */
class StreamExecLastRow(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    inputSchema: BaseRowSchema,
    outputSchema: BaseRowSchema,
    uniqueKeys: Array[Int],
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = outputSchema.relDataType

  def getUniqueKeys: Array[Int] = uniqueKeys

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecLastRow(
      cluster,
      traitSet,
      inputs.get(0),
      inputSchema,
      outputSchema,
      uniqueKeys,
      ruleDescription)
  }

  override def isDeterministic: Boolean = true

  override def producesUpdates: Boolean = true

  override def consumesRetractions: Boolean = true

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = true

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputNames = inputSchema.relDataType.getFieldNames
    val outputNames = outputSchema.relDataType.getFieldNames
    super.explainTerms(pw)
      .item("key", uniqueKeys.map(inputNames.get(_)).mkString(", "))
      .item("select", outputNames.mkString(", "))
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    val tableConfig = tableEnv.getConfig

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)

    val rowTimeFieldIndex = inputSchema.fieldTypeInfos.zipWithIndex
      .filter(e => FlinkTypeFactory.isRowtimeIndicatorType(e._1))
      .map(_._2)
    if (rowTimeFieldIndex.size() > 1) {
      throw new RuntimeException("More than one row time field. Currently this is not supported!")
    }
    val orderIndex = if (rowTimeFieldIndex.isEmpty) {
      -1
    } else {
      rowTimeFieldIndex.head
    }

    val operator = if (tableConfig.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)) {
      val processFunction = new MiniBatchLastRowFunction(
        rowTypeInfo,
        generateRetraction,
        orderIndex,
        tableConfig)

      new KeyedBundleOperator(
        processFunction,
        AggregateUtil.getMiniBatchTrigger(tableConfig),
        rowTypeInfo,
        tableConfig.getConf.getBoolean(
          TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
    } else {
      val processFunction = new LastRowFunction(
        rowTypeInfo,
        generateRetraction,
        orderIndex,
        tableConfig)

      val operator = new KeyedProcessOperator[BaseRow, BaseRow, BaseRow](processFunction)
      operator.setRequireState(true)
      operator
    }

    val ret = new OneInputTransformation(
      inputTransform,
      getOperatorName,
      operator,
      rowTypeInfo,
      inputTransform.getParallelism
    )
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    val selector = StreamExecUtil.getKeySelector(uniqueKeys, rowTypeInfo)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private def getOperatorName: String = {
    val inputNames = inputSchema.relDataType.getFieldNames
    val keyNames = uniqueKeys.map(inputNames.get(_)).mkString(", ")
    val outputNames = outputSchema.relDataType.getFieldNames.mkString(", ")
    s"LastRow: (key: ($keyNames), select: ($outputNames))"
  }
}
