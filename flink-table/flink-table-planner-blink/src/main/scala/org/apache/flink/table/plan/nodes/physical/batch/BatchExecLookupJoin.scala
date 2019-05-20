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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.streaming.api.transformations.StreamTransformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableConfigOptions}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.common.CommonLookupJoin
import org.apache.flink.table.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.sources.TableSource

import java.util

import scala.collection.JavaConversions._

/**
  * Batch physical RelNode for temporal table join.
  */
class BatchExecLookupJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    tableSource: TableSource[_],
    tableRowType: RelDataType,
    tableCalcProgram: Option[RexProgram],
    joinInfo: JoinInfo,
    joinType: JoinRelType)
  extends CommonLookupJoin(
    cluster,
    traitSet,
    input,
    tableSource,
    tableRowType,
    tableCalcProgram,
    joinInfo,
    joinType)
  with BatchPhysicalRel
  with BatchExecNode[BaseRow] {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchExecLookupJoin(
      cluster,
      traitSet,
      inputs.get(0),
      tableSource,
      tableRowType,
      tableCalcProgram,
      joinInfo,
      joinType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getDamBehavior: DamBehavior = DamBehavior.PIPELINED

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[BatchTableEnvironment, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
    tableEnv: BatchTableEnvironment): StreamTransformation[BaseRow] = {

    val inputTransformation = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val defaultParallelism = tableEnv.getConfig.getConf
      .getInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM)
    val transformation = translateToPlanInternal(
      inputTransformation,
      tableEnv.streamEnv,
      tableEnv.config,
      tableEnv.getRelBuilder)
    transformation.setParallelism(defaultParallelism)
    transformation
  }
}
