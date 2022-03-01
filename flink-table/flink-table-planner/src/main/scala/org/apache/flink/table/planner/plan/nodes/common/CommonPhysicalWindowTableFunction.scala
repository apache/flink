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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.logical.{CumulativeWindowSpec, HoppingWindowSpec, TimeAttributeWindowingStrategy, TumblingWindowSpec}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import scala.collection.JavaConverters._

/**
 * Base physical RelNode for window table-valued function.
 */
abstract class CommonPhysicalWindowTableFunction(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    val windowing: TimeAttributeWindowingStrategy)
  extends SingleRel(cluster, traitSet, inputRel) {

  override def deriveRowType(): RelDataType = outputRowType

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputFieldNames = getInput.getRowType.getFieldNames.asScala.toArray
    super.explainTerms(pw)
      .item("window", windowing.toSummaryString(inputFieldNames))
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val childRowCnt = mq.getRowCount(this.getInput)
    if (childRowCnt != null) {
      windowing.getWindow match {
        case _: TumblingWindowSpec => childRowCnt
        case hoppingWindow: HoppingWindowSpec =>
          val windowsNum = (hoppingWindow.getSize.toMillis /
            hoppingWindow.getSlide.toMillis).asInstanceOf[Int]
          childRowCnt * windowsNum
        case cumulateWindow: CumulativeWindowSpec =>
          val maxWindowsNum = (cumulateWindow.getMaxSize.toMillis /
            cumulateWindow.getStep.toMillis).asInstanceOf[Int]
          childRowCnt * maxWindowsNum
        case windowSpec =>
          throw new TableException(s"Unknown window spec: ${windowSpec.getClass.getSimpleName}")
      }
    } else {
      null.asInstanceOf[Double]
    }
  }
}
