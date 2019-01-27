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

import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.types.{DataTypes, TypeConverters}
import org.apache.flink.table.api.{BatchTableEnvironment, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonScan
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.{FlinkPhysicalRel, PhysicalTableSourceScan}
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.sources.{BatchTableSource, LimitableTableSource, TableSourceUtil}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode

/**
  * Flink RelNode to read data from an external source defined by a [[BatchTableSource]].
  */
class BatchExecTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: FlinkRelOptTable)
  extends PhysicalTableSourceScan(cluster, traitSet, relOptTable)
  with BatchPhysicalRel
  with BatchExecScan {

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    if (rowCnt == null) {
      return null
    }
    val cpu = 0
    val size = rowCnt * mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, cpu, size)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def copy(
      traitSet: RelTraitSet,
      relOptTable: FlinkRelOptTable): PhysicalTableSourceScan = {
    new BatchExecTableSourceScan(cluster, traitSet, relOptTable)
  }

  override def isDeterministic: Boolean = true

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def accept(visitor: BatchExecNodeVisitor): Unit = visitor.visit(this)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
    * Internal method, translates the [[org.apache.flink.table.plan.nodes.exec.BatchExecNode]]
    * into a Batch operator.
    *
    * @param tableEnv The [[BatchTableEnvironment]] of the translated Table.
    */
  override def translateToPlanInternal(
      tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      None)

    val config = tableEnv.getConfig
    val input = getSourceTransformation(tableEnv.streamEnv).asInstanceOf[StreamTransformation[Any]]
    assignSourceResourceAndParallelism(tableEnv, input)

    // check that declared and actual type of table source DataSet are identical
    if (TypeConverters.createInternalTypeFromTypeInfo(input.getOutputType) !=
        tableSource.getReturnType.toInternalType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getCanonicalName} " +
          s"returned a DataSet of type ${input.getOutputType} that does not match with the " +
          s"type ${tableSource.getReturnType} declared by the TableSource.getReturnType() " +
          s"method. Please validate the implementation of the TableSource.")
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression: Option[RexNode] = TableSourceUtil.getRowtimeExtractionExpression(
      tableSource,
      None,
      cluster,
      tableEnv.getRelBuilder,
      DataTypes.TIMESTAMP
    )

    convertToInternalRow(tableEnv, input, fieldIndexes,
      getRowType, tableSource.getReturnType, getTable.getQualifiedName, config, rowtimeExpression)
  }

  override def needInternalConversion: Boolean = {
    val fieldIndexes = TableSourceUtil.computeIndexMapping(
      tableSource,
      isStreamTable = false,
      None)
    hasTimeAttributeField(fieldIndexes) ||
        needsConversion(tableSource.getReturnType,
          CommonScan.extractTableSourceTypeClass(tableSource))
  }

  override private[flink] def getSourceTransformation(
      streamEnv: StreamExecutionEnvironment): StreamTransformation[_] = {
    tableSource.asInstanceOf[BatchTableSource[_]].getBoundedStream(streamEnv).getTransformation
  }

  private[flink] def canLimitPushedDown: Boolean = {
    tableSource match {
      case source: LimitableTableSource if source.isLimitPushedDown => true
      case _ => false
    }
  }
}
