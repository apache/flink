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
package org.apache.flink.table.plan.nodes.exec

import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.cost.FlinkBatchCost
import org.apache.flink.table.util.Logging

import java.{lang, util}

import scala.collection.JavaConversions._

/**
  * Base class for [[BatchExecNode]].
  */
trait BaseBatchExecNode[T] extends BatchExecNode[T] with Logging {

  private lazy val inputNodes: util.List[ExecNode[BatchTableEnvironment, _]] =
    new util.ArrayList[ExecNode[BatchTableEnvironment, _]](
      getFlinkPhysicalRel.getInputs.map(_.asInstanceOf[ExecNode[BatchTableEnvironment, _]]))

  override def getInputNodes: util.List[ExecNode[BatchTableEnvironment, _]] = inputNodes

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[BatchTableEnvironment, _]): Unit = {
    require(ordinalInParent >= 0 && ordinalInParent < inputNodes.size())
    inputNodes.set(ordinalInParent, newInputNode)
  }

  override def getEstimatedRowCount: lang.Double = {
    val rel = getFlinkPhysicalRel
    rel.getCluster.getMetadataQuery.getRowCount(rel)
  }

  override def getEstimatedTotalMem: lang.Double = {
    val rel = getFlinkPhysicalRel
    val relCost = rel.getCluster.getMetadataQuery.getNonCumulativeCost(rel)
    relCost match {
      case execCost: FlinkBatchCost => execCost.memory
      case _ => 0D // TODO should return null
    }
  }

  override def getEstimatedAverageRowSize: lang.Double = {
    val rel = getFlinkPhysicalRel
    rel.getCluster.getMetadataQuery.getAverageRowSize(rel)
  }
}

trait RowBatchExecNode extends BaseBatchExecNode[BaseRow]
