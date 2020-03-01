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
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.{isProctimeIndicatorType, isRowtimeIndicatorType}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.planner.plan.utils.{InputRefVisitor, KeySelectorUtil, RelExplainUtil, TemporalJoinUtil}
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector
import org.apache.flink.table.runtime.operators.join.temporal.{TemporalProcessTimeJoinOperator, TemporalRowTimeJoinOperator}
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo, JoinRelType}
import org.apache.calcite.rex._

import java.util

import scala.collection.JavaConversions._

class StreamExecTemporalJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = {
    val nonEquiJoinRex = getJoinInfo.getRemaining(cluster.getRexBuilder)

    var rowtimeJoin: Boolean = false
    val visitor = new RexVisitorImpl[Unit](true) {
      override def visitCall(call: RexCall): Unit = {
        if (call.getOperator == TEMPORAL_JOIN_CONDITION) {
          rowtimeJoin = TemporalJoinUtil.isRowtimeCall(call)
        } else {
          call.getOperands.foreach(node => node.accept(this))
        }
      }
    }
    nonEquiJoinRex.accept(visitor)
    rowtimeJoin
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
      .asInstanceOf[Transformation[BaseRow]]
    val rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftTransform,
      rightTransform,
      getRelDetailedDescription,
      joinOperator,
      BaseRowTypeInfo.of(returnType),
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
  * @param rightTimeAttributeInputReference is defined only for event time joins.
  */
class StreamExecTemporalJoinToCoProcessTranslator private (
  textualRepresentation: String,
  config: TableConfig,
  returnType: RowType,
  leftInputType: RowType,
  rightInputType: RowType,
  joinInfo: JoinInfo,
  rexBuilder: RexBuilder,
  leftTimeAttributeInputReference: Int,
  rightTimeAttributeInputReference: Option[Int],
  remainingNonEquiJoinPredicates: RexNode) {

  val nonEquiJoinPredicates: Option[RexNode] = Some(remainingNonEquiJoinPredicates)

  def getLeftKeySelector: BaseRowKeySelector = {
    KeySelectorUtil.getBaseRowSelector(
      joinInfo.leftKeys.toIntArray,
      BaseRowTypeInfo.of(leftInputType)
    )
  }

  def getRightKeySelector: BaseRowKeySelector = {
    KeySelectorUtil.getBaseRowSelector(
      joinInfo.rightKeys.toIntArray,
      BaseRowTypeInfo.of(rightInputType)
    )
  }

  def getJoinOperator(
    joinType: JoinRelType,
    returnFieldNames: Seq[String]): TwoInputStreamOperator[BaseRow, BaseRow, BaseRow] = {

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
  : TwoInputStreamOperator[BaseRow, BaseRow, BaseRow] = {

    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxRetentionTime = tableConfig.getMaxIdleStateRetentionTime
    joinType match {
      case JoinRelType.INNER =>
        if (rightTimeAttributeInputReference.isDefined) {
          new TemporalRowTimeJoinOperator(
            BaseRowTypeInfo.of(leftInputType),
            BaseRowTypeInfo.of(rightInputType),
            generatedJoinCondition,
            leftTimeAttributeInputReference,
            rightTimeAttributeInputReference.get,
            minRetentionTime,
            maxRetentionTime)
        } else {
          new TemporalProcessTimeJoinOperator(
            BaseRowTypeInfo.of(rightInputType),
            generatedJoinCondition,
            minRetentionTime,
            maxRetentionTime)
        }
      case _ =>
        throw new ValidationException(
          s"Only ${JoinRelType.INNER} temporal join is supported in [$textualRepresentation]")
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

    checkState(
      !joinInfo.isEqui,
      "Missing %s in join condition",
      TEMPORAL_JOIN_CONDITION)

    val leftType = FlinkTypeFactory.toLogicalRowType(leftInput.getRowType)
    val rightType = FlinkTypeFactory.toLogicalRowType(rightInput.getRowType)
    val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftType.getFieldCount,
      joinInfo,
      rexBuilder)

    val remainingNonEquiJoinPredicates = temporalJoinConditionExtractor.apply(nonEquiJoinRex)

    checkState(
      temporalJoinConditionExtractor.leftTimeAttribute.isDefined &&
        temporalJoinConditionExtractor.rightPrimaryKeyExpression.isDefined,
      "Missing %s in join condition",
      TEMPORAL_JOIN_CONDITION)

    new StreamExecTemporalJoinToCoProcessTranslator(
      textualRepresentation,
      config,
      returnType,
      leftType,
      rightType,
      joinInfo,
      rexBuilder,
      extractInputReference(
        temporalJoinConditionExtractor.leftTimeAttribute.get,
        textualRepresentation),
      temporalJoinConditionExtractor.rightTimeAttribute.map(
        rightTimeAttribute =>
          extractInputReference(
            rightTimeAttribute,
            textualRepresentation
          ) - leftType.getFieldCount),
      remainingNonEquiJoinPredicates)
  }

  private def extractInputReference(rexNode: RexNode, textualRepresentation: String): Int = {
    val inputReferenceVisitor = new InputRefVisitor
    rexNode.accept(inputReferenceVisitor)
    checkState(
      inputReferenceVisitor.getFields.length == 1,
      "Failed to find input reference in [%s]",
      textualRepresentation)
    inputReferenceVisitor.getFields.head
  }

  private class TemporalJoinConditionExtractor(
    textualRepresentation: String,
    rightKeysStartingOffset: Int,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder)

    extends RexShuttle {

    var leftTimeAttribute: Option[RexNode] = None

    var rightTimeAttribute: Option[RexNode] = None

    var rightPrimaryKeyExpression: Option[RexNode] = None

    override def visitCall(call: RexCall): RexNode = {
      if (call.getOperator != TEMPORAL_JOIN_CONDITION) {
        return super.visitCall(call)
      }

      checkState(
        leftTimeAttribute.isEmpty
          && rightPrimaryKeyExpression.isEmpty
          && rightTimeAttribute.isEmpty,
        "Multiple %s functions in [%s]",
        TEMPORAL_JOIN_CONDITION,
        textualRepresentation)

      if (TemporalJoinUtil.isRowtimeCall(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightTimeAttribute = Some(call.getOperands.get(1))

        rightPrimaryKeyExpression = Some(validateRightPrimaryKey(call.getOperands.get(2)))

        if (!isRowtimeIndicatorType(rightTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non rowtime timeAttribute [${rightTimeAttribute.get.getType}] " +
              s"used to create TemporalTableFunction")
        }
        if (!isRowtimeIndicatorType(leftTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non rowtime timeAttribute [${leftTimeAttribute.get.getType}] " +
              s"passed as the argument to TemporalTableFunction")
        }
      }
      else if (TemporalJoinUtil.isProctimeCall(call)) {
        leftTimeAttribute = Some(call.getOperands.get(0))
        rightPrimaryKeyExpression = Some(validateRightPrimaryKey(call.getOperands.get(1)))

        if (!isProctimeIndicatorType(leftTimeAttribute.get.getType)) {
          throw new ValidationException(
            s"Non processing timeAttribute [${leftTimeAttribute.get.getType}] " +
              s"passed as the argument to TemporalTableFunction")
        }
      }
      else {
        throw new IllegalStateException(
          s"Unsupported invocation $call in [$textualRepresentation]")
      }
      rexBuilder.makeLiteral(true)
    }

    private def validateRightPrimaryKey(rightPrimaryKey: RexNode): RexNode = {
      if (joinInfo.rightKeys.size() != 1) {
        throw new ValidationException(
          s"Only single column join key is supported. " +
            s"Found ${joinInfo.rightKeys} in [$textualRepresentation]")
      }
      val rightJoinKeyInputReference = joinInfo.rightKeys.get(0) + rightKeysStartingOffset

      val rightPrimaryKeyInputReference = extractInputReference(
        rightPrimaryKey,
        textualRepresentation)

      if (rightPrimaryKeyInputReference != rightJoinKeyInputReference) {
        throw new ValidationException(
          s"Join key [$rightJoinKeyInputReference] must be the same as " +
            s"temporal table's primary key [$rightPrimaryKey] " +
            s"in [$textualRepresentation]")
      }

      rightPrimaryKey
    }
  }
}
