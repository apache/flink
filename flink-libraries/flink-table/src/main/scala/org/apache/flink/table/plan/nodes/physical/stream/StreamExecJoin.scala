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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.types.{RowType, TypeConverters}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.ProjectionCodeGenerator.generateProjection
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.{BaseRow, BinaryRow, JoinedRow}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.util.{FlinkRexUtil, JoinUtil, StreamExecUtil}
import org.apache.flink.table.runtime.join.stream._
import org.apache.flink.table.runtime.join.stream.bundle._
import org.apache.flink.table.runtime.join.stream.state.JoinStateHandler
import org.apache.flink.table.runtime.join.stream.state.`match`.JoinMatchStateHandler
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Pair
import org.apache.calcite.util.mapping.IntPair

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StreamExecJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    rowRelDataType: RelDataType,
    val joinCondition: RexNode,
    joinRowType: RelDataType,
    val joinInfo: JoinInfo,
    val filterNulls: Array[Boolean],
    keyPairs: List[IntPair],
    val joinType: FlinkJoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      filterNulls,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  def inferPrimaryKeyAndJoinStateType(
    input: RelNode,
    joinKeys: Array[Int],
    isMiniBatchEnabled: Boolean): (Option[Array[Int]], JoinStateHandler.Type) = {
    val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(input)
    var (pk, stateType) = if (uniqueKeys != null && uniqueKeys.nonEmpty) {
      var primaryKey = uniqueKeys.head.toArray
      val joinKeyIsPk = uniqueKeys.exists { pk =>
        primaryKey = pk.toArray
        pk.forall(joinKeys.contains(_))
      }
      if (joinKeyIsPk) {
        (Some(primaryKey), JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY)
      } else {
        //select tiny pk
        uniqueKeys.foreach(pk =>
          if (primaryKey.length > pk.length()) primaryKey = pk.toArray)
        (Some(primaryKey), JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY)
      }
    } else {
      (None, JoinStateHandler.Type.WITHOUT_PRIMARY_KEY)
    }
    // if join type is semi/anti and without non equal pred, set right state type to count type.
    if (input.equals(getRight) &&
        (joinType.equals(FlinkJoinRelType.ANTI) || joinType.equals(FlinkJoinRelType.SEMI)) &&
      joinInfo.isEqui &&
      isMiniBatchEnabled
    ) {
      // COUNT_KEY_SIZE only used for miniBatch stream stream join
      stateType = JoinStateHandler.Type.COUNT_KEY_SIZE
    }
    (pk, stateType)
  }

  override def producesUpdates: Boolean = {
    joinType != FlinkJoinRelType.INNER && joinType != FlinkJoinRelType.SEMI
  }

  override def producesRetractions: Boolean = {
    joinType match {
      case FlinkJoinRelType.FULL | FlinkJoinRelType.RIGHT | FlinkJoinRelType.LEFT => true
      case FlinkJoinRelType.ANTI => true
      case _ => false
    }
  }

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = {
    def getCurrentRel(node: RelNode): RelNode = {
      node match {
        case _: HepRelVertex => node.asInstanceOf[HepRelVertex].getCurrentRel
        case _ => node
      }
    }

    val realInput = getCurrentRel(input)
    val inputUniqueKeys = cluster.getMetadataQuery.getUniqueKeys(realInput)
    if (inputUniqueKeys != null) {
      val joinKeys = if (input == getCurrentRel(left)) {
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

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString)
      .item("join", joinSelectionToString)
      .item("joinType", joinTypeToString)
      .itemIf("joinHint", joinHint, joinHint != null)
  }

  @VisibleForTesting
  def explainJoin: JList[Pair[String, AnyRef]] = {
    val (lStateType, lMatchStateType, rStateType, rMatchStateType) =
      // set isMiniBatch enabled to false, since plan tests don't care whether it is miniBatch.
      getJoinAllStateType(false)
    val values = new JArrayList[Pair[String, AnyRef]]
    values.add(Pair.of("where", joinConditionToString))
    values.add(Pair.of("join", joinSelectionToString))
    values.add(Pair.of("joinType", joinTypeToString))
    values.add(Pair.of("leftStateType", s"$lStateType"))
    values.add(Pair.of("leftMatchStateType", s"$lMatchStateType"))
    values.add(Pair.of("rightStateType", s"$rStateType"))
    values.add(Pair.of("rightMatchStateType", s"$rMatchStateType"))
    values
  }

  override def isDeterministic: Boolean = FlinkRexUtil.isDeterministicOperator(joinCondition)

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  /**
   * Translates the StreamExecNode into a Flink operator.
   *
   * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
   * @return DataStream of type expectedType or RowTypeInfo
   */
  override def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {
    val tableConfig = tableEnv.getConfig

    val returnType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType)

    // get the equality keys
    val (leftKeys, rightKeys) =
      JoinUtil.checkAndGetKeys(keyPairs, getLeft, getRight, allowEmpty = true)

    val leftTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightTransform = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val leftType = leftTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val rightType = rightTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]

    val leftSelect = StreamExecUtil.getKeySelector(leftKeys.toArray, leftType)
    val rightSelect = StreamExecUtil.getKeySelector(rightKeys.toArray, rightType)

    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val lPkProj = generatePrimaryKeyProjection(tableConfig, left, leftType, leftKeys.toArray)
    val rPkProj = generatePrimaryKeyProjection(tableConfig, right, rightType, rightKeys.toArray)

    val isMiniBatchEnabled = tableConfig.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
    val (lStateType, lMatchStateType, rStateType, rMatchStateType) =
      getJoinAllStateType(isMiniBatchEnabled)
    val condFunc = generateConditionFunction(tableConfig, leftType, rightType)
    val leftIsAccRetract = StreamExecRetractionRules.isAccRetract(left)
    val rightIsAccRetract = StreamExecRetractionRules.isAccRetract(right)

    val operator = if (isMiniBatchEnabled && tableConfig.getConf.getBoolean(
      TableConfigOptions.SQL_EXEC_MINIBATCH_JOIN_ENABLED)) {
      joinType match {
        case FlinkJoinRelType.INNER =>
          new MiniBatchInnerJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            JoinUtil.getMiniBatchTrigger(tableConfig),
            tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.LEFT =>
          new MiniBatchLeftOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            JoinUtil.getMiniBatchTrigger(tableConfig),
            tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.RIGHT =>
          new MiniBatchRightOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            JoinUtil.getMiniBatchTrigger(tableConfig),
            tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.FULL =>
          new MiniBatchFullOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            filterNulls,
            JoinUtil.getMiniBatchTrigger(tableConfig),
            tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
        case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
          new MiniBatchAntiSemiJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            inferMatchStateTypeBase(lStateType),
            rMatchStateType,
            leftIsAccRetract,
            rightIsAccRetract,
            joinType.equals(FlinkJoinRelType.SEMI),
            joinInfo.isEqui,
            filterNulls,
            JoinUtil.getMiniBatchTrigger(tableConfig),
            tableConfig.getConf.getBoolean(
              TableConfigOptions.SQL_EXEC_MINI_BATCH_FLUSH_BEFORE_SNAPSHOT))
      }
    } else {
      joinType match {
        case FlinkJoinRelType.INNER =>
          new InnerJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            filterNulls)
        case FlinkJoinRelType.LEFT =>
          new LeftOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.RIGHT =>
          new RightOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.FULL =>
          new FullOuterJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            lMatchStateType,
            rMatchStateType,
            filterNulls)
        case FlinkJoinRelType.ANTI | FlinkJoinRelType.SEMI =>
          new SemiAntiJoinStreamOperator(
            leftType,
            rightType,
            condFunc,
            leftSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            rightSelect.asInstanceOf[KeySelector[BaseRow, BaseRow]],
            lPkProj,
            rPkProj,
            lStateType,
            rStateType,
            maxRetentionTime,
            minRetentionTime,
            joinType.equals(FlinkJoinRelType.ANTI),
            inferMatchStateType(lStateType),
            !rightIsAccRetract,
            filterNulls)
      }
    }

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftTransform,
      rightTransform,
      JoinUtil.joinToString(joinRowType, joinCondition, joinType, getExpressionString),
      operator,
      returnType,
      leftTransform.getParallelism)

    if (leftKeys.isEmpty) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }
    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
    ret
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case FlinkJoinRelType.INNER => "InnerJoin"
    case FlinkJoinRelType.LEFT => "LeftOuterJoin"
    case FlinkJoinRelType.RIGHT => "RightOuterJoin"
    case FlinkJoinRelType.FULL => "FullOuterJoin"
    case FlinkJoinRelType.SEMI => "SemiJoin"
    case FlinkJoinRelType.ANTI => "AntiJoin"
  }

  private[flink] def generatePrimaryKeyProjection(
      config: TableConfig,
      input: RelNode,
      inputType: BaseRowTypeInfo, keys: Array[Int]): GeneratedProjection = {

    val isMiniBatchEnabled = config.getConf.contains(
      TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY)
    val (pk, _) = inferPrimaryKeyAndJoinStateType(input, keys, isMiniBatchEnabled)

    if (pk.nonEmpty) {
      val pkType = {
        val types = inputType.getFieldTypes
        new BaseRowTypeInfo(pk.get.map(types(_)): _*)
      }
      generateProjection(
        CodeGeneratorContext(config),
        "PkProjection",
        TypeConverters.createInternalTypeFromTypeInfo(inputType).asInstanceOf[RowType],
        TypeConverters.createInternalTypeFromTypeInfo(pkType).asInstanceOf[RowType],
        pk.get,
        reusedOutRecord = false)
    } else {
      null
    }
  }

  private[flink] def generateConditionFunction(
      config: TableConfig,
      leftType: BaseRowTypeInfo,
      rightType: BaseRowTypeInfo): GeneratedJoinConditionFunction = {
    val ctx = CodeGeneratorContext(config)
    // should consider null fields
    val exprGenerator = new ExprCodeGenerator(ctx, false, true)
        .bindInput(TypeConverters.createInternalTypeFromTypeInfo(leftType))
        .bindSecondInput(TypeConverters.createInternalTypeFromTypeInfo(rightType))

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(cluster.getRexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinConditionFunction(
      ctx,
      "ConditionFunction",
      body, config)
  }

  private[flink] def getJoinAllStateType(isMiniBatchEnabled: Boolean):
  (JoinStateHandler.Type,
    JoinMatchStateHandler.Type,
    JoinStateHandler.Type,
    JoinMatchStateHandler.Type) = {
    // get the equality keys
    val (leftKeys, rightKeys) =
      JoinUtil.checkAndGetKeys(keyPairs, getLeft, getRight, allowEmpty = true)
    val (_, lStateType) =
      inferPrimaryKeyAndJoinStateType(getLeft, leftKeys.toArray, isMiniBatchEnabled)
    val (_, rStateType) =
      inferPrimaryKeyAndJoinStateType(getRight, rightKeys.toArray, isMiniBatchEnabled)

    val (lStateMatchType, rStateMatchType) = joinType match {
      case FlinkJoinRelType.INNER =>
        (JoinMatchStateHandler.Type.EMPTY_MATCH, JoinMatchStateHandler.Type.EMPTY_MATCH)

      case FlinkJoinRelType.LEFT =>
        (inferMatchStateType(lStateType), JoinMatchStateHandler.Type.EMPTY_MATCH)

      case FlinkJoinRelType.RIGHT =>
        (JoinMatchStateHandler.Type.EMPTY_MATCH, inferMatchStateType(rStateType))

      case FlinkJoinRelType.FULL =>
        (inferMatchStateType(lStateType), inferMatchStateType(rStateType))

      case FlinkJoinRelType.SEMI | FlinkJoinRelType.ANTI =>
        (inferMatchStateType(lStateType), JoinMatchStateHandler.Type.EMPTY_MATCH)
    }

    (lStateType, lStateMatchType, rStateType, rStateMatchType)
  }

  private[flink] def inferMatchStateType(
      inputSideStateType: JoinStateHandler.Type): JoinMatchStateHandler.Type = {

    var matchType = inferMatchStateTypeBase(inputSideStateType)
    //if joinType is Semi and the right side don't send retraction message. then semi operator
    // won't keep the match counts.
    if (joinType.equals(FlinkJoinRelType.SEMI) && !StreamExecRetractionRules.isAccRetract(right)) {
      matchType = JoinMatchStateHandler.Type.EMPTY_MATCH
    }
    matchType
  }

  private[flink] def inferMatchStateTypeBase(inputSideStateType: JoinStateHandler.Type):
  JoinMatchStateHandler.Type = {

    if (joinInfo.isEqui) {
      //only equality condition
      JoinMatchStateHandler.Type.ONLY_EQUALITY_CONDITION_EMPTY_MATCH
    } else {
      inputSideStateType match {
        case JoinStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY =>
          JoinMatchStateHandler.Type.JOIN_KEY_CONTAIN_PRIMARY_KEY_MATCH

        case JoinStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY =>
          JoinMatchStateHandler.Type.JOIN_KEY_NOT_CONTAIN_PRIMARY_KEY_MATCH

        case _ =>
          //match more than one time
          JoinMatchStateHandler.Type.WITHOUT_PRIMARY_KEY_MATCH
      }
    }
  }
}

