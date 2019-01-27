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
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.codegen.ValuesCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.RowBatchExecNode
import org.apache.flink.table.plan.nodes.exec.batch.BatchExecNodeVisitor
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexLiteral

import scala.collection.JavaConversions._

/**
  * StreamTransformation RelNode for a LogicalValues.
  */
class BatchExecValues(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    rowRelDataType: RelDataType,
    tuples: ImmutableList[ImmutableList[RexLiteral]],
    description: String)
  extends Values(cluster, rowRelDataType, tuples, traitSet)
  with BatchPhysicalRel
  with RowBatchExecNode {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new BatchExecValues(
      cluster,
      traitSet,
      getRowType,
      getTuples,
      description
    )
  }

  override def deriveRowType(): RelDataType = rowRelDataType

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("values", getRowType.getFieldNames.toList.mkString(", "))
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
    val inputFormat = ValuesCodeGenerator.generatorInputFormat(
      tableEnv,
      getRowType,
      tuples,
      description)
    val transformation = tableEnv.streamEnv
      .createInput(inputFormat, inputFormat.getProducedType, description)
      .getTransformation
    tableEnv.getRUKeeper.addTransformation(this, transformation)
    transformation.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)
    transformation
  }

}

