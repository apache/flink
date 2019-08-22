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
import org.apache.flink.streaming.api.transformations.TwoInputTransformation
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{JoinUtil, KeySelectorUtil, RelExplainUtil}
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
import org.apache.flink.table.runtime.operators.join.stream.{StreamingJoinOperator, StreamingSemiAntiJoinOperator}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for regular [[Join]].
  *
  * Regular joins are the most generic type of join in which any new records or changes to
  * either side of the join input are visible and are affecting the whole join result.
  */
class StreamExecJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = {
    flinkJoinType != FlinkJoinType.INNER && flinkJoinType != FlinkJoinType.SEMI
  }

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    def getCurrentRel(rel: RelNode): RelNode = {
      rel match {
        case _: HepRelVertex => rel.asInstanceOf[HepRelVertex].getCurrentRel
        case _ => rel
      }
    }

    val realInput = getCurrentRel(input)
    val inputUniqueKeys = getCluster.getMetadataQuery.getUniqueKeys(realInput)
    if (inputUniqueKeys != null) {
      val joinKeys = if (input == getCurrentRel(getLeft)) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val pkContainJoinKey = inputUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
      if (pkContainJoinKey) false else true
    } else {
      true
    }
  }

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = {
    flinkJoinType match {
      case FlinkJoinType.FULL | FlinkJoinType.RIGHT | FlinkJoinType.LEFT => true
      case FlinkJoinType.ANTI => true
      case _ => false
    }
  }

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecJoin(cluster, traitSet, left, right, conditionExpr, joinType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item("leftInputSpec", analyzeJoinInput(left))
      .item("rightInputSpec", analyzeJoinInput(right))
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {

    val tableConfig = planner.getTableConfig
    val returnType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]
    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val leftType = leftTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val rightType = rightTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]

    val (leftJoinKey, rightJoinKey) =
      JoinUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight, allowEmptyKey = true)

    val leftSelect = KeySelectorUtil.getBaseRowSelector(leftJoinKey, leftType)
    val rightSelect = KeySelectorUtil.getBaseRowSelector(rightJoinKey, rightType)

    val leftInputSpec = analyzeJoinInput(left)
    val rightInputSpec = analyzeJoinInput(right)

    val generatedCondition = JoinUtil.generateConditionFunction(
      tableConfig,
      cluster.getRexBuilder,
      getJoinInfo,
      leftType.toRowType,
      rightType.toRowType)

    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime

    val operator = if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI) {
      new StreamingSemiAntiJoinOperator(
        joinType == JoinRelType.ANTI,
        leftType,
        rightType,
        generatedCondition,
        leftInputSpec,
        rightInputSpec,
        filterNulls,
        minRetentionTime)
    } else {
      val leftIsOuter = joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL
      val rightIsOuter = joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL
      new StreamingJoinOperator(
        leftType,
        rightType,
        generatedCondition,
        leftInputSpec,
        rightInputSpec,
        leftIsOuter,
        rightIsOuter,
        filterNulls,
        minRetentionTime)
    }

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftTransform,
      rightTransform,
      getRelDetailedDescription,
      operator,
      returnType,
      leftTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)
    ret
  }

  private def analyzeJoinInput(input: RelNode): JoinInputSideSpec = {
    val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(input)
    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      JoinInputSideSpec.withoutUniqueKey()
    } else {
      val inRowType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(input.getRowType))
      val joinKeys = if (input == left) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val uniqueKeysContainedByJoinKey = uniqueKeys
        .filter(uk => uk.toArray.forall(joinKeys.contains(_)))
        .map(_.toArray)
        .toArray
      if (uniqueKeysContainedByJoinKey.nonEmpty) {
        // join key contains unique key
        val smallestUniqueKey = getSmallestKey(uniqueKeysContainedByJoinKey)
        val uniqueKeySelector = KeySelectorUtil.getBaseRowSelector(smallestUniqueKey, inRowType)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(uniqueKeyTypeInfo, uniqueKeySelector)
      } else {
        val smallestUniqueKey = getSmallestKey(uniqueKeys.map(_.toArray).toArray)
        val uniqueKeySelector = KeySelectorUtil.getBaseRowSelector(smallestUniqueKey, inRowType)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKey(uniqueKeyTypeInfo, uniqueKeySelector)
      }
    }
  }

  private def getSmallestKey(keys: Array[Array[Int]]): Array[Int] = {
    var smallest = keys.head
    for (key <- keys) {
      if (key.length < smallest.length) {
        smallest = key
      }
    }
    smallest
  }
}
