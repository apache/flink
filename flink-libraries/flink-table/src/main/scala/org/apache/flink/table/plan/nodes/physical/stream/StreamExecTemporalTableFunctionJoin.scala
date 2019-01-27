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

import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator
import org.apache.flink.streaming.api.transformations.{StreamTransformation, TwoInputTransformation}
import org.apache.flink.table.api.types.RowType
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableException, ValidationException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.calcite.FlinkTypeFactory.{isProctimeIndicatorType, isRowtimeIndicatorType}
import org.apache.flink.table.codegen.CodeGeneratorContext.DEFAULT_COLLECTOR_TERM
import org.apache.flink.table.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator, GeneratedFunction}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow, JoinedRow}
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.nodes.exec.RowStreamExecNode
import org.apache.flink.table.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.plan.util.JoinUtil.{joinConditionToString, joinSelectionToString, joinTypeToString}
import org.apache.flink.table.plan.util.TemporalJoinUtil.TEMPORAL_JOIN_CONDITION
import org.apache.flink.table.plan.util.{FlinkRexUtil, JoinUtil, RexDefaultVisitor, TemporalJoinUtil}
import org.apache.flink.table.runtime.join.{TemporalProcessTimeJoin, TemporalRowtimeJoin}
import org.apache.flink.table.runtime.{BaseRowKeySelector, BinaryRowKeySelector}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex._

import scala.collection.JavaConversions._

/**
  * RelNode for a stream join with [[org.apache.flink.table.api.functions.TemporalTableFunction]].
  */
class StreamExecTemporalTableFunctionJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    rightNode: RelNode,
    joinCondition: RexNode,
    joinInfo: JoinInfo,
    leftSchema: BaseRowSchema,
    rightSchema: BaseRowSchema,
    schema: BaseRowSchema,
    joinType: FlinkJoinRelType,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
  with StreamPhysicalRel
  with RowStreamExecNode {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString(schema.relDataType, joinCondition, getExpressionString))
      .item("join", joinSelectionToString(schema.relDataType))
      .item("joinType", joinTypeToString(joinType))
  }

  override def producesRetractions: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    checkState(inputs.size() == 2)
    new StreamExecTemporalTableFunctionJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinCondition,
      joinInfo,
      leftSchema,
      rightSchema,
      schema,
      joinType,
      ruleDescription)
  }

  override def isDeterministic: Boolean = FlinkRexUtil.isDeterministicOperator(joinCondition)

  override def getFlinkPhysicalRel: FlinkPhysicalRel = this

  override def translateToPlanInternal(
    tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    validateKeyTypes()

    val returnType = FlinkTypeFactory.toInternalBaseRowTypeInfo(getRowType)

    val joinTranslator = StreamExecTemporalJoinToCoProcessTranslator.create(
      this.toString,
      tableEnv.getConfig,
      schema.internalType(),
      leftSchema,
      rightSchema,
      joinInfo,
      cluster.getRexBuilder)

    val joinOperator = joinTranslator.getJoinOperator(
      joinType,
      schema.fieldNames,
      ruleDescription)

    val leftKeySelector = joinTranslator.getLeftKeySelector()
    val rightKeySelector = joinTranslator.getRightKeySelector()

    val joinOpName = JoinUtil.joinToString(getRowType, joinCondition, joinType, getExpressionString)

    val leftTransform = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]
    val rightTransform = getInputNodes.get(1).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val ret = new TwoInputTransformation[BaseRow, BaseRow, BaseRow](
      leftTransform,
      rightTransform,
      joinOpName,
      joinOperator,
      returnType.asInstanceOf[BaseRowTypeInfo],
      leftTransform.getParallelism)

    ret.setResources(getResource.getReservedResourceSpec,
      getResource.getPreferResourceSpec)

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftKeySelector, rightKeySelector)
    ret.setStateKeyType(leftKeySelector.asInstanceOf[ResultTypeQueryable[_]].getProducedType)
    ret
  }

  private def validateKeyTypes(): Unit = {
    // at least one equality expression
    val leftFields = left.getRowType.getFieldList
    val rightFields = right.getRowType.getFieldList

    joinInfo.pairs().toList.foreach(pair => {
      val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
      val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName
      // check if keys are compatible
      if (leftKeyType != rightKeyType) {
        throw new TableException(
          "Equality join predicate on incompatible types.\n" +
            s"\tLeft: $left,\n" +
            s"\tRight: $right,\n" +
            s"\tCondition: (${joinConditionToString(
              schema.relDataType,
              joinCondition, getExpressionString)})"
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
  leftSchema: BaseRowSchema,
  rightSchema: BaseRowSchema,
  joinInfo: JoinInfo,
  rexBuilder: RexBuilder,
  leftTimeAttributeInputReference: Int,
  rightTimeAttributeInputReference: Option[Int],
  remainingNonEquiJoinPredicates: RexNode) {

  val nonEquiJoinPredicates: Option[RexNode] = Some(remainingNonEquiJoinPredicates)

  def getLeftKeySelector(): BaseRowKeySelector = {
    new BinaryRowKeySelector(
      joinInfo.leftKeys.toIntArray,
      leftSchema.typeInfo())
  }

  def getRightKeySelector(): BaseRowKeySelector = {
    new BinaryRowKeySelector(
      joinInfo.rightKeys.toIntArray,
      rightSchema.typeInfo())
  }

  def getJoinOperator(
    joinType: FlinkJoinRelType,
    returnFieldNames: Seq[String],
    ruleDescription: String): TwoInputStreamOperator[BaseRow, BaseRow, BaseRow] = {

    val leftType = leftSchema.internalType()
    val rightType = rightSchema.internalType()

    // input must not be nullable, because the runtime join function will make sure
    // the code-generated function won't process null inputs
    val ctx = CodeGeneratorContext(config)
    val exprGenerator = new ExprCodeGenerator(
      ctx,
      nullableInput = false,
      config.getNullCheck)
      .bindInput(leftType)
      .bindSecondInput(rightType)

    val conversion = exprGenerator.generateConverterResultExpression(
      returnType, classOf[GenericRow])

    val body = if (nonEquiJoinPredicates.isEmpty) {
      // only equality condition
      s"""
         |${conversion.code}
         |$DEFAULT_COLLECTOR_TERM.collect(${conversion.resultTerm});
         |""".stripMargin
    } else {
      val condition = exprGenerator.generateExpression(nonEquiJoinPredicates.get)
      s"""
         |${condition.code}
         |if (${condition.resultTerm}) {
         |  ${conversion.code}
         |  $DEFAULT_COLLECTOR_TERM.collect(${conversion.resultTerm});
         |}
         |""".stripMargin
    }

    FunctionCodeGenerator

    val genFunction = FunctionCodeGenerator.generateFunction(
      ctx,
      ruleDescription,
      classOf[FlatJoinFunction[BaseRow, BaseRow, BaseRow]],
      body,
      returnType,
      leftType,
      config,
      input2Type = Some(rightType))
      .asInstanceOf[GeneratedFunction[FlatJoinFunction[BaseRow, BaseRow, BaseRow], BaseRow]]

    createJoinOperator(joinType, genFunction)
  }

  protected def createJoinOperator(
    joinType: FlinkJoinRelType,
    joinFunction: GeneratedFunction[FlatJoinFunction[BaseRow, BaseRow, BaseRow], BaseRow])
  : TwoInputStreamOperator[BaseRow, BaseRow, BaseRow] = {

    joinType match {
      case FlinkJoinRelType.INNER =>
        if (rightTimeAttributeInputReference.isDefined) {
          new TemporalRowtimeJoin(
            leftSchema.typeInfo().asInstanceOf[TypeInformation[BaseRow]],
            rightSchema.typeInfo().asInstanceOf[TypeInformation[BaseRow]],
            joinFunction.name,
            joinFunction.code,
            leftTimeAttributeInputReference,
            rightTimeAttributeInputReference.get)
        }
        else {
          new TemporalProcessTimeJoin(
            leftSchema.typeInfo().asInstanceOf[TypeInformation[BaseRow]],
            rightSchema.typeInfo().asInstanceOf[TypeInformation[BaseRow]],
            joinFunction.name,
            joinFunction.code)
        }
      case _ =>
        throw new ValidationException(
          s"Only ${FlinkJoinRelType.INNER} temporal join is supported in [$textualRepresentation]")
    }
  }
}

object StreamExecTemporalJoinToCoProcessTranslator {
  def create(
    textualRepresentation: String,
    config: TableConfig,
    returnType: RowType,
    leftSchema: BaseRowSchema,
    rightSchema: BaseRowSchema,
    joinInfo: JoinInfo,
    rexBuilder: RexBuilder): StreamExecTemporalJoinToCoProcessTranslator = {

    checkState(
      !joinInfo.isEqui,
      "Missing %s in join condition",
      TEMPORAL_JOIN_CONDITION)

    val nonEquiJoinRex: RexNode = joinInfo.getRemaining(rexBuilder)
    val temporalJoinConditionExtractor = new TemporalJoinConditionExtractor(
      textualRepresentation,
      leftSchema.arity,
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
      leftSchema,
      rightSchema,
      joinInfo,
      rexBuilder,
      extractInputReference(
        temporalJoinConditionExtractor.leftTimeAttribute.get,
        textualRepresentation),
      temporalJoinConditionExtractor.rightTimeAttribute.map(
        rightTimeAttribute =>
          extractInputReference(rightTimeAttribute, textualRepresentation) - leftSchema.arity),
      remainingNonEquiJoinPredicates)
  }

  private def extractInputReference(rexNode: RexNode, textualRepresentation: String): Int = {
    val inputReferenceVisitor = new InputReferenceVisitor(textualRepresentation)
    rexNode.accept(inputReferenceVisitor)
    checkState(
      inputReferenceVisitor.inputReference.isDefined,
      "Failed to find input reference in [%s]",
      textualRepresentation)
    inputReferenceVisitor.inputReference.get
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

  /**
    * Extracts input references from RexNode.
    */
  private class InputReferenceVisitor(textualRepresentation: String)
    extends RexDefaultVisitor[RexNode] {

    var inputReference: Option[Int] = None

    override def visitInputRef(inputRef: RexInputRef): RexNode = {
      inputReference = Some(inputRef.getIndex)
      inputRef
    }

    override def visitNode(rexNode: RexNode): RexNode = {
      throw new ValidationException(
        s"Unsupported expression [$rexNode] in [$textualRepresentation]. Expected input reference")
    }
  }
}
