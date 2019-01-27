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

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.calcite.Rank
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.RankUtil._
import org.apache.flink.table.plan.util.{FlinkRelOptUtil, FlinkRexUtil, RankRange, RankUtil, StreamExecUtil}
import org.apache.flink.table.runtime.KeyedProcessOperator
import org.apache.flink.table.runtime.rank._
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.{SqlKind, SqlRankFunction}
import org.apache.calcite.util.ImmutableBitSet

import java.util

import scala.collection.JavaConverters._

class StreamExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputNode: RelNode,
    inputSchema: BaseRowSchema,
    schema: BaseRowSchema,
    rankFunction: SqlRankFunction,
    partitionKey: Array[Int],
    sortCollation: RelCollation,
    rankRange: RankRange,
    val outputRankFunColumn: Boolean)
  extends Rank(
    cluster,
    traitSet,
    inputNode,
    rankFunction,
    ImmutableBitSet.of(partitionKey: _*),
    sortCollation,
    rankRange)
  with StreamPhysicalRel
  with RowStreamExecNode {

  var strategy: RankStrategy = _

  def getStrategy(
      tableConfig: Option[TableConfig] = None,
      forceRecompute: Boolean = false): RankStrategy = {
    if (strategy == null || forceRecompute) {
      val tc: TableConfig = tableConfig.getOrElse(FlinkRelOptUtil.getTableConfig(this))
      strategy = RankUtil.analyzeRankStrategy(cluster, tc, this, sortCollation)
    }
    strategy
  }

  override def producesUpdates = true

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode): Boolean =
    getStrategy(forceRecompute = true) == RetractRank

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    val rank = new StreamExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      inputSchema,
      schema,
      rankFunction,
      partitionKey,
      sortCollation,
      rankRange,
      outputRankFunColumn)
    rank.strategy = this.strategy
    rank
  }

  private def partitionFieldsToString(partitionKey: Array[Int], rowType: RelDataType): String = {
    partitionKey.map(rowType.getFieldNames.get(_)).mkString(", ")
  }

  private def selectToString: String = {
    if (inputSchema.arity == schema.arity) {
      "*"
    } else {
      "*, rowNum"
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput)
      .item("rankFunction", rankFunction.getKind)
      .itemIf("partitionBy",
        partitionFieldsToString(partitionKey, schema.relDataType),
        partitionKey.nonEmpty)
      .item("orderBy", Rank.sortFieldsToString(sortCollation, schema.relDataType))
      .item("rankRange", rankRange.toString(inputSchema.fieldNames))
      .item("strategy", getStrategy())
      .item("select", selectToString)
  }

  override def isDeterministic: Boolean = FlinkRexUtil.isDeterministicOperator(rankFunction)

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig
    val rankKind = rankFunction.getKind match {
      case SqlKind.ROW_NUMBER => SqlKind.ROW_NUMBER
      case SqlKind.RANK =>
        throw new TableException("RANK() on streaming table is not supported currently")
      case SqlKind.DENSE_RANK =>
        throw new TableException("DENSE_RANK() on streaming table is not supported currently")
      case k =>
        throw new TableException(s"Streaming tables do not support $k rank function.")
    }

    val inputTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val inputRowTypeInfo = new BaseRowTypeInfo(inputSchema.fieldTypeInfos: _*)
    val fieldCollation = sortCollation.getFieldCollations.asScala
    val sortKeySelector = createSortKeySelector(fieldCollation, inputSchema)
    val (sortKeyType, sorter) = createSortKeyTypeAndSorter(inputSchema, fieldCollation)

    val generateRetraction = StreamExecRetractionRules.isAccRetract(this)
    val cacheSize = tableConfig.getConf.getLong(TableConfigOptions.SQL_EXEC_TOPN_CACHE_SIZE)

    val processFunction = getStrategy(Some(tableConfig), forceRecompute = true) match {
      case AppendFastRank =>
        new AppendRankFunction(
          inputRowTypeInfo,
          sortKeyType,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          tableConfig)

      case UpdateFastRank(primaryKeys) =>
        val rowKeyType = createRowKeyType(primaryKeys, inputSchema)
        val rowKeySelector = createKeySelector(primaryKeys, inputSchema)
        new UpdateRankFunction(
          inputRowTypeInfo,
          rowKeyType,
          rowKeySelector,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          tableConfig)

      case UnaryUpdateRank(primaryKeys) =>
        // unary update rank requires a key selector that returns key of other types rather
        // than BaseRow
        val genSortKeyExtractor = getUnarySortKeyExtractor(fieldCollation, inputSchema)
        val unarySortKeyType = inputSchema.fieldTypes(fieldCollation.head.getFieldIndex)
        val rowKeyType = createRowKeyType(primaryKeys, inputSchema)
        val rowKeySelector = createKeySelector(primaryKeys, inputSchema)
        new UnarySortUpdateRankFunction[Any](
          inputRowTypeInfo,
          rowKeyType,
          unarySortKeyType,
          rowKeySelector,
          genSortKeyExtractor,
          getOrderFromFieldCollation(fieldCollation.head),
          schema.arity,
          rankKind,
          rankRange,
          cacheSize,
          generateRetraction,
          tableConfig)

      case RetractRank =>
        new RetractRankFunction(
          inputRowTypeInfo,
          sortKeyType,
          sorter,
          sortKeySelector.asInstanceOf[KeySelector[BaseRow, BaseRow]],
          schema.arity,
          rankKind,
          rankRange,
          generateRetraction,
          tableConfig)
    }
    val outputBaseInfo = schema.typeInfo().asInstanceOf[BaseRowTypeInfo]
    val rankOpName = getOperatorName

    val inputTypeInfo = inputSchema.typeInfo()
    val selector = StreamExecUtil.getKeySelector(partitionKey, inputTypeInfo)

    val operator = new KeyedProcessOperator(processFunction)
    operator.setRequireState(true)
    val ret = new OneInputTransformation(
      inputTransform,
      rankOpName,
      operator,
      outputBaseInfo,
      inputTransform.getParallelism)

    if (partitionKey.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }

  private def getOperatorName: String = {
    var result =
      s"${getStrategy()}(orderBy: (${Rank.sortFieldsToString(sortCollation, schema.relDataType)})"
    if (partitionKey.nonEmpty) {
      result += s", partitionBy: (${partitionFieldsToString(partitionKey, schema.relDataType)})"
    }
    result += s", $selectToString"
    result += s", ${rankRange.toString(inputSchema.fieldNames)})"
    result
  }
}
