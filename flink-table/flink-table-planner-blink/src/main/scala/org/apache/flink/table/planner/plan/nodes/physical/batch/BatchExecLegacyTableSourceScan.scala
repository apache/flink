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


import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction
import org.apache.flink.table.api.TableException
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.CodeGeneratorContext
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.PhysicalLegacyTableSourceScan
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.planner.sources.TableSourceUtil
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.sources.{DefinedFieldMapping, StreamTableSource}
import org.apache.flink.table.utils.TypeMappingUtils

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

import java.util.function.{Function => JFunction}
import java.{lang, util}

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode to read data from an external source defined by a
  * bounded [[StreamTableSource]].
  */
class BatchExecLegacyTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    tableSourceTable: LegacyTableSourceTable[_])
  extends PhysicalLegacyTableSourceScan(cluster, traitSet, tableSourceTable)
          with BatchPhysicalRel
          with BatchExecNode[RowData]{

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLegacyTableSourceScan(cluster, traitSet, tableSourceTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    if (rowCnt == null) {
      return null
    }
    val cpu = 0
    val rowSize = mq.getAverageRowSize(this)
    val size = rowCnt * rowSize
    planner.getCostFactory.makeCost(rowCnt, cpu, size)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchPlanner, _]] = List()

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val config = planner.getTableConfig
    val inputTransform = getSourceTransformation(planner.getExecEnv)

    val fieldIndexes = computeIndexMapping()

    val inputDataType = inputTransform.getOutputType
    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataStream are identical
    if (inputDataType != TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(producedDataType)) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
        s"returned a DataStream of data type $inputDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeAttributeDescriptor(
      tableSource,
      tableSourceTable.getRowType
    ).map(desc =>
      TableSourceUtil.getRowtimeExtractionExpression(
        desc.getTimestampExtractor,
        producedDataType,
        planner.getRelBuilder,
        nameMapping
      )
    )

    if (needInternalConversion) {
      // the produced type may not carry the correct precision user defined in DDL, because
      // it may be converted from legacy type. Fix precision using logical schema from DDL.
      // code generation requires the correct precision of input fields.
      val fixedProducedDataType = TableSourceUtil.fixPrecisionForProducedDataType(
        tableSource,
        FlinkTypeFactory.toLogicalRowType(tableSourceTable.getRowType))
      ScanUtil.convertToInternalRow(
        CodeGeneratorContext(config),
        inputTransform.asInstanceOf[Transformation[Any]],
        fieldIndexes,
        fixedProducedDataType,
        getRowType,
        getTable.getQualifiedName,
        config,
        rowtimeExpression)
    } else {
      inputTransform.asInstanceOf[Transformation[RowData]]
    }

  }

  def needInternalConversion: Boolean = {
    val fieldIndexes = computeIndexMapping()
    ScanUtil.hasTimeAttributeField(fieldIndexes) ||
      ScanUtil.needsConversion(tableSource.getProducedDataType)
  }

  def getEstimatedRowCount: lang.Double = {
    getCluster.getMetadataQuery.getRowCount(this)
  }

  override def createInput[IN](
      env: StreamExecutionEnvironment,
      format: InputFormat[IN, _ <: InputSplit],
      t: TypeInformation[IN]): Transformation[IN] = {
    // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
    // paths. If read partitioned source, after partition pruning, we need let InputFormat
    // to read multiple partitions which are multiple paths.
    // We can use InputFormatSourceFunction directly to support InputFormat.
    val func = new InputFormatSourceFunction[IN](format, t)
    ExecNode.setManagedMemoryWeight(env.addSource(func, tableSource.explainSource(), t)
        .getTransformation)
  }

  private def computeIndexMapping()
    : Array[Int] = {
    TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
      tableSource,
      FlinkTypeFactory.toTableSchema(getRowType).getTableColumns,
      false,
      nameMapping
    )
  }

  private lazy val nameMapping: JFunction[String, String] = tableSource match {
    case mapping: DefinedFieldMapping if mapping.getFieldMapping != null =>
      new JFunction[String, String] {
        override def apply(t: String): String = mapping.getFieldMapping.get(t)
      }
    case _ => JFunction.identity()
  }
}
