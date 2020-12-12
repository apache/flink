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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.ChainingStrategy
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil
import org.apache.flink.table.planner.plan.nodes.exec.{LegacyBatchExecNode, ExecEdge, ExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{BatchMultipleInputStreamOperatorFactory, TableOperatorWrapperGenerator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * Batch physical node for [[MultipleInputRel]].
 *
 * @param inputRels the input rels of multiple input rel,
 *                  which are not a part of the multiple input rel.
 * @param outputRel the root rel of the sub-graph of the multiple input rel.
 * @param inputEdges the [[ExecEdge]]s related to each input.
 */
class BatchExecMultipleInput(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode,
    inputEdges: Array[ExecEdge])
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel, inputEdges.map(_.getPriority))
  with LegacyBatchExecNode[RowData]
  with BatchPhysicalRel {

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputEdges: util.List[ExecEdge] = inputEdges.toList

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[_]): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def translateToPlanInternal(
      planner: BatchPlanner): Transformation[RowData] = {
    val inputTransforms = getInputNodes.map(n => n.translateToPlan(planner))
    val tailTransform = outputRel.asInstanceOf[LegacyBatchExecNode[_]].translateToPlan(planner)

    val outputType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val generator = new TableOperatorWrapperGenerator(inputTransforms, tailTransform, readOrders)
    generator.generate()

    val inputTransformAndInputSpecPairs = generator.getInputTransformAndInputSpecPairs
    val multipleInputTransform = new MultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new BatchMultipleInputStreamOperatorFactory(
        inputTransformAndInputSpecPairs.map(_.getValue),
        generator.getHeadWrappers,
        generator.getTailWrapper),
      outputType,
      generator.getParallelism)

    // add inputs as the order of input specs
    inputTransformAndInputSpecPairs.foreach(input => multipleInputTransform.addInput(input.getKey))

    if (generator.getMaxParallelism > 0) {
      multipleInputTransform.setMaxParallelism(generator.getMaxParallelism)
    }

    // set resources
    multipleInputTransform.setResources(generator.getMinResources, generator.getPreferredResources)
    val memoryKB = generator.getManagedMemoryWeight
    ExecNodeUtil.setManagedMemoryWeight(multipleInputTransform, memoryKB * 1024L)

    // set chaining strategy for source chaining
    multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES)

    multipleInputTransform
  }

}
