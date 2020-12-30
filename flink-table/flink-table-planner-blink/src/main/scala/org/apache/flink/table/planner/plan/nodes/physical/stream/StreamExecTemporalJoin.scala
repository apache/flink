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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.transformations.TwoInputTransformation
import org.apache.flink.table.api.{TableConfig, TableException, ValidationException}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.LegacyStreamExecNode
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.{TEMPORAL_JOIN_CONDITION, TEMPORAL_JOIN_CONDITION_PRIMARY_KEY}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector
import org.apache.flink.table.runtime.operators.join.temporal.{TemporalProcessTimeJoinOperator, TemporalRowTimeJoinOperator}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rex._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Stream physical node for temporal table join (FOR SYSTEM_TIME AS OF) and
 * temporal TableFunction join (LATERAL TemporalTableFunction(o.proctime)).
 *
 * <p>The legacy temporal table function join is the subset of temporal table join,
 * the only difference is the validation, we reuse most same logic here.
 */
class StreamExecTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with LegacyStreamExecNode[RowData] {

  override def requireWatermark: Boolean = {
    TemporalJoinUtil.isRowTimeJoin(cluster.getRexBuilder, getJoinInfo)
  }

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecTemporalJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override protected def translateToPlanInternal(
    planner: StreamPlanner): Transformation[RowData] = {

    validateKeyTypes()

    val returnType = FlinkTypeFactory.toLogicalRowType(getRowType)

    val joinTranslator = StreamExecTemporalJoinToCoProcessTranslator.create(
      this.toString,
      planner.getTableConfig,
      returnType,
      leftRel,
      rightRel,
      getJoinInfo,
      cluster.getRexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(joinType, returnType.getFieldNames)
    val leftKeySelector = joinTranslator.getLeftKeySelector
    val rightKeySelector = joinTranslator.getRightKeySelector

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val ret = new TwoInputTransformation[RowData, RowData, RowData](
      leftTransform,
      rightTransform,
      getRelDetailedDescription,
      joinOperator,
      InternalTypeInfo.of(returnType),
      leftTransform.getParallelism)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftKeySelector, rightKeySelector)
    ret.setStateKeyType(leftKeySelector.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
    ret
  }

  private def validateKeyTypes(): Unit = {
    // at least one equality expression
    val leftFields = left.getRowType.getFieldList
    val rightFields = right.getRowType.getFieldList

    getJoinInfo.pairs().toList.foreach(pair => {
      val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
      val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName
      // check if keys are compatible
      if (leftKeyType != rightKeyType) {
        throw new TableException(
          "Equality join predicate on incompatible types.\n" +
            s"\tLeft: $left,\n" +
            s"\tRight: $right,\n" +
            s"\tCondition: (${RelExplainUtil.expressionToString(
              getCondition, inputRowType, getExpressionString)})"
        )
      }
    })
  }
}

/**
  * @param rightRowTimeAttributeInputReference is defined only for event time joins.
  */
class StreamExecTemporalJoinToCoProcessTranslator private(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftInputType: RowType,
    rightInputType: RowType,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder,
    leftTimeAttributeInputReference: Int,
    rightRowTimeAttributeInputReference: Option[Int],
    remainingNonEquiJoinPredicates: RexNode,
    isTemporalFunctionJoin: Boolean) {

  val nonEquiJoinPredicates: Option[RexNode] = Some(remainingNonEquiJoinPredicates)

  def getLeftKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.leftKeys.toIntArray,
      InternalTypeInfo.of(leftInputType)
    )
  }

  def getRightKeySelector: RowDataKeySelector = {
    KeySelectorUtil.getRowDataSelector(
      joinInfo.rightKeys.toIntArray,
      InternalTypeInfo.of(rightInputType)
    )
  }

  def getJoinOperator(
    joinType: JoinRelType,
    returnFieldNames: Seq[String]): TwoInputStreamOperator[RowData, RowData, RowData] = {

    // input must not be nullable, because the runtime join function will make sure
    // the code-generated function won't process null inputs
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(ctx, nullableInput = false)
      .bindInput(leftInputType)
      .bindSecondInput(rightInputType)

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      "return true;"
    } else {
      val condition = exprGenerator.generateExpression(nonEquiJoinPredicates.get)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    val generatedJoinCondition = FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)

    createJoinOperator(config, joinType, generatedJoinCondition)
  }

  protected def createJoinOperator(
    tableConfig: TableConfig,
    joinType: JoinRelType,
    generatedJoinCondition: GeneratedJoinCondition)
  : TwoInputStreamOperator[RowData, RowData, RowData] = {

    if (isTemporalFunctionJoin) {
      if (joinType != JoinRelType.INNER) {
        throw new ValidationException(
          "Temporal table function join currently only support INNER JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    } else {
      if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
        throw new TableException(
          "Temporal table join currently only support INNER JOIN and LEFT JOIN, " +
            "but was " + joinType.toString + " JOIN.")
      }
    }

    val isLeftOuterJoin = joinType == JoinRelType.LEFT
    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    if (rightRowTimeAttributeInputReference.isDefined) {
      new TemporalRowTimeJoinOperator(
        InternalTypeInfo.of(leftInputType),
        InternalTypeInfo.of(rightInputType),
        generatedJoinCondition,
        leftTimeAttributeInputReference,
        rightRowTimeAttributeInputReference.get,
        minRetentionTime,
        maxRetentionTime,
        isLeftOuterJoin)
    } else {
      if (isTemporalFunctionJoin) {
        new TemporalProcessTimeJoinOperator(
          InternalTypeInfo.of(rightInputType),
          generatedJoinCondition,
          minRetentionTime,
          maxRetentionTime,
          isLeftOuterJoin)
      } else {
        // The exsiting TemporalProcessTimeJoinOperator has already supported temporal table join.
        // However, the semantic of this implementation is problematic, because the join processing
        // for left stream doesn't wait for the complete snapshot of temporal table, this may
        // mislead users in production environment. See FLINK-19830 for more details.
        throw new TableException("Processing-time temporal join is not supported yet.")
      }
    }
  }
}

object StreamExecTemporalJoinToCoProcessTranslator {
  def create(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftInput: RelNode,
    rightInput: RelNode,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder): StreamExecTemporalJoinToCoProcessTranslator = {

    val leftType = FlinkTypeFactory.toLogicalRowType(leftInput.getRowType)
    val rightType = FlinkTypeFactory.toLogicalRowType(rightInput.getRowType)
    val isTemporalFunctionJoin = TemporalJoinUtil.isTemporalFunctionJoin(rexBuilder, joinInfo)

    checkState(
      !joinInfo.isEqui,
      "Missing %s in temporal join condition",
      TEMPORAL_JOIN_CONDITION)

    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftType.getFieldCount,
      joinInfo,
      rexBuilder,
      isTemporalFunctionJoin)

    val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
    val remainingNonEquiJoinPredicates = temporalJoinConditionExtractor.apply(nonEquiJoinRex)

    val (leftTimeAttributeInputRef, rightRowTimeAttributeInputRef: Option[Int]) =
      if (TemporalJoinUtil.isRowTimeJoin(rexBuilder, joinInfo)) {
        checkState(temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
          temporalJoinConditionExtractor.rightPrimaryKey.isDefined,
          "Missing %s in Event-Time temporal join condition", TEMPORAL_JOIN_CONDITION)

        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get, textualRepresentation)
        val rightTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.rightTimeAttribute.get, textualRepresentation)
        val rightInputRef = rightTimeAttributeInputRef - leftType.getFieldCount

        (leftTimeAttributeInputRef, Some(rightInputRef))
      } else {
        val leftTimeAttributeInputRef = TemporalJoinUtil.extractInputRef(
          temporalJoinConditionExtractor.leftTimeAttribute.get, textualRepresentation)
        // right time attribute defined in temporal join condition iff in Event time join
        (leftTimeAttributeInputRef, None)
      }

    new StreamExecTemporalJoinToCoProcessTranslator(
      textualRepresentation,
      config,
      returnType,
      leftType,
      rightType,
      joinInfo,
      rexBuilder,
      leftTimeAttributeInputRef,
      rightRowTimeAttributeInputRef,
      remainingNonEquiJoinPredicates,
      isTemporalFunctionJoin)
  }

  private class TemporalJoinConditionExtractor(
    textualRepresentation: String,
    rightKeysStartingOffset: Int,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder,
    isTemporalFunctionJoin: Boolean) extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKey: Option[Array[RexNode]] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }

      // at most one temporal function in a temporal join node
      if (isTemporalFunctionJoin) {
        checkState(
          leftTimeAttribute.isEmpty
            && rightPrimaryKey.isEmpty
            && rightTimeAttribute.isEmpty,
          "Multiple %s temporal functions in [%s]",
          TEMPORAL_JOIN_CONDITION,
          textualRepresentation)
      }

      if (TemporalJoinUtil.isRowTimeTemporalTableJoinCon(call) ||
        TemporalJoinUtil.isRowTimeTemporalFunctionJoinCon(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightTimeAttribute = Some(call.getOperands.get(1))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(2)))
      } else {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightPrimaryKey = Some(extractPrimaryKeyArray(call.getOperands.get(1)))
      }

      // the condition of temporal function comes from WHERE clause,
      // so it's not been validated in logical plan
      if (isTemporalFunctionJoin) {
        TemporalJoinUtil.validateTemporalFunctionCondition(
          call,
          leftTimeAttribute.get,
          rightTimeAttribute,
          rightPrimaryKey,
          rightKeysStartingOffset,
          joinInfo,
          "Temporal Table Function")
      }

      rexBuilder.makeLiteral(true)
    }

    private def extractPrimaryKeyArray(rightPrimaryKey: RexNode): Array[RexNode] = {
      if (!rightPrimaryKey.isInstanceOf[RexCall] ||
        rightPrimaryKey.asInstanceOf[RexCall].getOperator != TEMPORAL_JOIN_CONDITION_PRIMARY_KEY) {
        throw new ValidationException(
          s"No primary key [${rightPrimaryKey.asInstanceOf[RexCall]}] " +
            s"defined in versioned table of Event-time temporal table join")
       }
      rightPrimaryKey.asInstanceOf[RexCall].getOperands.asScala.toArray
     }
  }
}
