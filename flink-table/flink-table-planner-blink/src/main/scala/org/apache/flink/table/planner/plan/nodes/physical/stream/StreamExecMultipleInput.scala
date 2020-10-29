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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.nodes.physical.MultipleInputRel
import org.apache.flink.table.runtime.operators.multipleinput.{StreamMultipleInputStreamOperatorFactory, TableOperatorWrapperGenerator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * Stream physical node for [[MultipleInputRel]].
 *
 * @param inputRels the input rels of multiple input rel,
 *                  which are not a part of the multiple input rel.
 * @param outputRel the root rel of the sub-graph of the multiple input rel.
 */
class StreamExecMultipleInput(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRels: Array[RelNode],
    outputRel: RelNode)
  extends MultipleInputRel(cluster, traitSet, inputRels, outputRel, inputRels.map(_ => 0))
  with StreamExecNode[RowData]
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = {
    throw new UnsupportedOperationException()
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    throw new UnsupportedOperationException()
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val inputTransforms = getInputNodes.map(n => n.translateToPlan(planner))
    val tailTransform = outputRel.asInstanceOf[StreamExecNode[_]].translateToPlan(planner)

    val outputType = InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val generator = new TableOperatorWrapperGenerator(inputTransforms, tailTransform)
    generator.generate()

    val inputInfoList = generator.getInputInfoList
    val multipleInputTransform = new KeyedMultipleInputTransformation[RowData](
      getRelDetailedDescription,
      new StreamMultipleInputStreamOperatorFactory(
        inputInfoList.map(_.getInputSpec),
        generator.getHeadWrappers,
        generator.getTailWrapper),
      outputType,
      generator.getParallelism,
      Preconditions.checkNotNull(generator.getStateKeyType)
    )

    // add inputs as the order of input specs
    inputInfoList.foreach { info =>
      multipleInputTransform.addInput(info.getInputTransform, info.getKeySelector)
    }

    if (generator.getMaxParallelism > 0) {
      multipleInputTransform.setMaxParallelism(generator.getMaxParallelism)
    }

    // set resources
    multipleInputTransform.setResources(generator.getMinResources, generator.getPreferredResources)

    multipleInputTransform
  }
}

