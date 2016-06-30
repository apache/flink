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

package org.apache.flink.api.table.plan.nodes.dataset

import java.lang.Iterable

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.BatchTableEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with DataSetOperator.
  *
  */
class DataSetMinus(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    all: Boolean)
  extends BiRel(cluster, traitSet, left, right)
    with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetMinus(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      all
    )
  }

  override def toString: String = {
    s"SetMinus(setMinus: ($setMinusSelectionToString}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("setMinus", setMinusSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val children = this.getInputs
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + metadata.getRowCount(child)
    }

    planner.getCostFactory.makeCost(rowCnt, 0, 0)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    var leftDataSet: DataSet[Any] = null
    var rightDataSet: DataSet[Any] = null

    expectedType match {
      case None =>
        leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
        rightDataSet =
          right.asInstanceOf[DataSetRel].translateToPlan(tableEnv, Some(leftDataSet.getType))
      case _ =>
        leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv, expectedType)
        rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv, expectedType)
    }

    val leftType = leftDataSet.getType
    val rightType = rightDataSet.getType
    val coGroupedDs = leftDataSet.coGroup(rightDataSet)

    // If it is atomic type, the field expression need to be "*".
    // Otherwise, we use int-based field position keys
    val coGroupedPredicateDs =
    if (leftType.isTupleType || leftType.isInstanceOf[CompositeType[Any]]) {
      coGroupedDs.where(0 until left.getRowType.getFieldCount: _*)
    } else {
      coGroupedDs.where("*")
    }

    val coGroupedWithoutFunctionDs =
    if (rightType.isTupleType || rightType.isInstanceOf[CompositeType[Any]]) {
      coGroupedPredicateDs.equalTo(0 until right.getRowType.getFieldCount: _*)
    } else {
      coGroupedPredicateDs.equalTo("*")
    }

    coGroupedWithoutFunctionDs.`with`(new MinusCoGroupFunction[Any](all))
      .name(s"intersect: $setMinusSelectionToString")
  }

  private def setMinusSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}

class MinusCoGroupFunction[T](all: Boolean) extends CoGroupFunction[T, T, T] {
  override def coGroup(first: Iterable[T], second: Iterable[T], out: Collector[T]): Unit = {
    if (first == null || second == null) return
    val leftIter = first.iterator()
    val rightIter = second.iterator()

    if (all) {
      while (rightIter.hasNext && leftIter.hasNext) {
        leftIter.next()
        rightIter.next()
      }

      while (leftIter.hasNext) {
        out.collect(leftIter.next())
      }
    } else {
      if (!rightIter.hasNext && leftIter.hasNext) {
        out.collect(leftIter.next())
      }
    }
  }
}
