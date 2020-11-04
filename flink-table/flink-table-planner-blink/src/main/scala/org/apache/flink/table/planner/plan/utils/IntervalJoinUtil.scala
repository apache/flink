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

package org.apache.flink.table.planner.plan.utils

import java.util

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.utils.JoinedRowData
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RelTimeIndicatorConverter}
import org.apache.flink.table.planner.codegen._
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.types.logical.RowType

import scala.collection.JavaConversions._

/**
  * Util for interval join operator.
  */
object IntervalJoinUtil {

  case class WindowBounds(
      isEventTime: Boolean,
      leftLowerBound: Long,
      leftUpperBound: Long,
      leftTimeIdx: Int,
      rightTimeIdx: Int)

  protected case class WindowBound(bound: Long, isLeftLower: Boolean)

  protected case class TimePredicate(
      isEventTime: Boolean,
      leftInputOnLeftSide: Boolean,
      leftTimeIdx: Int,
      rightTimeIdx: Int,
      pred: RexCall)

  protected case class TimeAttributeAccess(isEventTime: Boolean, isLeftInput: Boolean, idx: Int)

  /**
    * Checks if an expression accesses a time attribute.
    *
    * @param expr      The expression to check.
    * @param inputType The input type of the expression.
    * @return True, if the expression accesses a time attribute. False otherwise.
    */
  def accessesTimeAttribute(expr: RexNode, inputType: RelDataType): Boolean = {
    expr match {
      case ref: RexInputRef =>
        val accessedType = inputType.getFieldList.get(ref.getIndex).getType
        FlinkTypeFactory.isTimeIndicatorType(accessedType)
      case c: RexCall =>
        c.operands.exists(accessesTimeAttribute(_, inputType))
      case _ => false
    }
  }

  /**
    * Generates a JoinFunction that applies additional join predicates and projects the result.
    *
    * @param  config          table env config
    * @param  joinType        join type to determine whether input can be null
    * @param  leftType        left stream type
    * @param  rightType       right stream type
    * @param  returnType      return type
    * @param  otherCondition  non-equi condition
    * @param  funcName        name of generated function
    */
  private[flink] def generateJoinFunction(
      config: TableConfig,
      joinType: JoinRelType,
      leftType: RowType,
      rightType: RowType,
      returnType: RelDataType,
      otherCondition: Option[RexNode],
      funcName: String): GeneratedFunction[FlatJoinFunction[RowData, RowData, RowData]] = {

    // whether input can be null
    val nullCheck = joinType match {
      case JoinRelType.INNER => false
      case JoinRelType.LEFT => true
      case JoinRelType.RIGHT => true
      case JoinRelType.FULL => true
    }

    val ctx = CodeGeneratorContext(config)
    val collectorTerm = CodeGenUtils.DEFAULT_COLLECTOR_TERM

    val returnTypeInfo = FlinkTypeFactory.toLogicalRowType(returnType)
    val joinedRow = "joinedRow"
    ctx.addReusableOutputRecord(returnTypeInfo, classOf[JoinedRowData], joinedRow)

    val exprGenerator = new ExprCodeGenerator(ctx, nullCheck)
      .bindInput(leftType)
      .bindSecondInput(rightType)

    val leftRow = CodeGenUtils.DEFAULT_INPUT1_TERM
    val rightRow = CodeGenUtils.DEFAULT_INPUT2_TERM
    val buildJoinedRow =
      s"""
         |$joinedRow.replace($leftRow,$rightRow);""".stripMargin

    val body = otherCondition match {
      case None =>
        s"""
           |$buildJoinedRow
           |$collectorTerm.collect($joinedRow);
           |""".stripMargin
      case Some(remainCondition) =>
        val genCond = exprGenerator.generateExpression(remainCondition)
        s"""
           |${genCond.code}
           |if (${genCond.resultTerm}) {
           |  $buildJoinedRow
           |  $collectorTerm.collect($joinedRow);
           |}""".stripMargin
    }

    FunctionCodeGenerator.generateFunction(
      ctx,
      funcName,
      classOf[FlatJoinFunction[RowData, RowData, RowData]],
      body,
      returnTypeInfo,
      leftType,
      input2Type = Some(rightType),
      collectorTerm = collectorTerm)
  }

  /**
    * Extracts the window bounds from a join predicate.
    *
    * @param  predicate           join predicate
    * @param  leftLogicalFieldCnt number of attributes on the left join input
    * @param  joinRowType         row type of the join result
    * @param  rexBuilder          RexBuilder
    * @param  config              TableConfig
    * @return A Tuple2 of extracted window bounds and remaining predicates.
    */
  private[flink] def extractWindowBoundsFromPredicate(
      predicate: RexNode,
      leftLogicalFieldCnt: Int,
      joinRowType: RelDataType,
      rexBuilder: RexBuilder,
      config: TableConfig): (Option[WindowBounds], Option[RexNode]) = {

    // Converts the condition to conjunctive normal form (CNF)
    val cnfCondition = FlinkRexUtil.toCnf(rexBuilder,
      config.getConfiguration.getInteger(FlinkRexUtil.TABLE_OPTIMIZER_CNF_NODES_LIMIT),
      predicate)

    // split the condition into time predicates and other predicates
    // We need two range predicates or an equality predicate for a properly bounded window join.
    val (timePreds, otherPreds) = cnfCondition match {
      case call: RexCall if cnfCondition.getKind == SqlKind.AND =>
        // extract all time predicates from conjunctive predicate
        call.getOperands
          .map(identifyTimePredicate(_, leftLogicalFieldCnt, joinRowType))
          .foldLeft((Seq[TimePredicate](), Seq[RexNode]()))((preds, analyzed) => {
            analyzed match {
              case Left(timePred) => (preds._1 :+ timePred, preds._2)
              case Right(otherPred) => (preds._1, preds._2 :+ otherPred)
            }
          })
      case c: RexCall =>
        // extract time predicate if it exists
        identifyTimePredicate(c, leftLogicalFieldCnt, joinRowType) match {
          case Left(timePred) => (Seq[TimePredicate](timePred), Seq[RexNode]())
          case Right(otherPred) => (Seq[TimePredicate](), Seq[RexNode](otherPred))
        }
      case _ =>
        // No valid window bounds.
        return (None, Some(predicate))
    }

    timePreds match {
      case Seq() =>
        return (None, Some(predicate))
      case Seq(t) if t.pred.getKind != SqlKind.EQUALS =>
        // single predicate must be equality predicate
        return (None, Some(predicate))
      case s@Seq(_, _) if s.exists(_.pred.getKind == SqlKind.EQUALS) =>
        // pair of range predicate must not include equals predicate
        return (None, Some(predicate))
      case Seq(_) =>
      // Single equality predicate is OK
      case Seq(_, _) =>
      // Two range (i.e., non-equality predicates are OK
      case _ =>
        return (None, Some(predicate))
    }

    // assemble window bounds from predicates
    val streamTimeOffsets = timePreds.map(computeWindowBoundFromPredicate(_, rexBuilder, config))
    val (leftLowerBound, leftUpperBound) =
      streamTimeOffsets match {
        case Seq(Some(x: WindowBound), Some(y: WindowBound)) if x.isLeftLower && !y.isLeftLower =>
          // two range predicates
          (x.bound, y.bound)
        case Seq(Some(x: WindowBound), Some(y: WindowBound)) if !x.isLeftLower && y.isLeftLower =>
          // two range predicates
          (y.bound, x.bound)
        case Seq(Some(x: WindowBound)) =>
          // single equality predicate
          (x.bound, x.bound)
        case _ =>
          // Window join requires two comparison predicate that bound the time in both directions.
          return (None, Some(predicate))
      }

    // compose the remain condition list into one condition
    val remainCondition =
      otherPreds match {
        case Seq() =>
          None
        case _ =>
          Some(otherPreds.reduceLeft((l, r) => RelOptUtil.andJoinFilters(rexBuilder, l, r)))
      }

    val bounds = if (timePreds.head.leftInputOnLeftSide) {
      Some(WindowBounds(
        timePreds.head.isEventTime,
        leftLowerBound,
        leftUpperBound,
        timePreds.head.leftTimeIdx,
        timePreds.head.rightTimeIdx))
    } else {
      Some(WindowBounds(
        timePreds.head.isEventTime,
        leftLowerBound,
        leftUpperBound,
        timePreds.head.rightTimeIdx,
        timePreds.head.leftTimeIdx))
    }

    (bounds, remainCondition)
  }

  /**
    * Analyzes a predicate and identifies whether it is a valid predicate for a window join.
    *
    * A valid window join predicate is a range or equality predicate (<, <=, ==, =>, >) that
    * accesses time attributes of both inputs, each input on a different side of the condition.
    * Both accessed time attributes must be of the same time type, i.e., row-time or proc-time.
    *
    * Examples:
    * - left.rowtime > right.rowtime + 2.minutes => valid
    * - left.rowtime == right.rowtime => valid
    * - left.proctime < right.rowtime + 2.minutes => invalid: different time type
    * - left.rowtime - right.rowtime < 2.minutes => invalid: both time attributes on same side
    *
    * If the predicate is a regular join predicate, i.e., it accesses no time attribute it is
    * returned as well.
    *
    * @return Either a valid time predicate (Left) or a valid non-time predicate (Right)
    */
  private def identifyTimePredicate(
      pred: RexNode,
      leftFieldCount: Int,
      inputRowType: RelDataType): Either[TimePredicate, RexNode] = {

    pred match {
      case c: RexCall =>
        c.getKind match {
          case SqlKind.GREATER_THAN |
               SqlKind.GREATER_THAN_OR_EQUAL |
               SqlKind.LESS_THAN |
               SqlKind.LESS_THAN_OR_EQUAL |
               SqlKind.EQUALS =>

            val leftTerm = c.getOperands.get(0)
            val rightTerm = c.getOperands.get(1)

            // validate that both sides of the condition do not access non-time attributes
            if (accessesNonTimeAttribute(leftTerm, inputRowType) ||
              accessesNonTimeAttribute(rightTerm, inputRowType)) {
              return Right(pred)
            }

            // get time attribute on left side of comparison
            val leftTimeAttrAccess =
              extractTimeAttributeAccesses(leftTerm, leftFieldCount, inputRowType) match {
                case Seq() => None
                case Seq(a) => Some(a)
                case _ =>
                  // Window join predicate may only access a single time attribute on each side.
                  return Right(pred)
              }

            // get time attribute on right side of comparison
            val rightTimeAccess =
              extractTimeAttributeAccesses(rightTerm, leftFieldCount, inputRowType) match {
                case Seq() => None
                case Seq(a) => Some(a)
                case _ =>
                  // Window join predicate may only access a single time attribute on each side.
                  return Right(pred)
              }

            // check if both sides of the condition access a time attribute,
            // if both join inputs are accessed, and
            // if both accesses are on the same time type (event-time or proc-time)
            (leftTimeAttrAccess, rightTimeAccess) match {
              case (None, None) =>
                // neither left or right accesses a time attribute. This is a regular join predicate
                Right(c)
              case (Some(_), None) | (None, Some(_)) =>
                // Both sides or a window join predicate must access a time attribute.
                Right(pred)
              case (Some(left), Some(right)) if left.isEventTime != right.isEventTime =>
                // Both time attributes in a window join predicate must be of the same type.
                Right(pred)
              case (Some(left), Some(right)) if left.isLeftInput == right.isLeftInput =>
                // Window join predicates must reference the time attribute of both inputs.
                Right(pred)
              case (Some(left), Some(right)) =>
                Left(TimePredicate(left.isEventTime, left.isLeftInput, left.idx, right.idx, c))
            }
          // not a comparison predicate.
          case _ => Right(pred)
        }
      case other =>
        Right(other)
    }
  }

  /**
    * Extracts all time attributes that are accessed in an expression.
    *
    * @return A Seq of all time attribute accessed in the expression.
    */
  private def extractTimeAttributeAccesses(
      expr: RexNode,
      leftFieldCount: Int,
      inputRowType: RelDataType): Seq[TimeAttributeAccess] = {

    expr match {
      case ref: RexInputRef =>
        // check if time attribute is accessed and from which input side
        val idx = ref.getIndex
        inputRowType.getFieldList.get(idx).getType match {
          case t: TimeIndicatorRelDataType =>
            // time attribute access. Remember time type and side of input
            if (idx < leftFieldCount) {
              Seq(TimeAttributeAccess(t.isEventTime, isLeftInput = true, idx))
            } else {
              Seq(TimeAttributeAccess(t.isEventTime, isLeftInput = false, idx - leftFieldCount))
            }
          case _ =>
            // not a time attribute access.
            Seq()
        }
      case c: RexCall =>
        // concat time-attributes of all operands
        c.operands
          .map(extractTimeAttributeAccesses(_, leftFieldCount, inputRowType))
          .reduce(_ ++ _)
      case _ => Seq()
    }
  }

  /**
    * Checks if an expression accesses a non-time attribute.
    *
    * @param expr      The expression to check.
    * @param inputType The input type of the expression.
    * @return True, if the expression accesses a non-time attribute. False otherwise.
    */
  private def accessesNonTimeAttribute(expr: RexNode, inputType: RelDataType): Boolean = {
    expr match {
      case ref: RexInputRef =>
        var accessedType: RelDataType = null
        try {
          accessedType = inputType.getFieldList.get(ref.getIndex).getType
        } catch {
          case e =>
            e.printStackTrace()
        }
        accessedType match {
          case _: TimeIndicatorRelDataType => false
          case _ => true
        }
      case c: RexCall =>
        c.operands.exists(accessesNonTimeAttribute(_, inputType))
      case _ => false
    }
  }

  /**
    * Computes the absolute bound on the left operand of a comparison expression and
    * whether the bound is an upper or lower bound.
    *
    * @return window boundary, is left lower bound
    */
  private def computeWindowBoundFromPredicate(
      timePred: TimePredicate,
      rexBuilder: RexBuilder,
      config: TableConfig): Option[WindowBound] = {

    val isLeftLowerBound: Boolean =
      timePred.pred.getKind match {
        case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL =>
          timePred.leftInputOnLeftSide
        case SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL =>
          !timePred.leftInputOnLeftSide
        case SqlKind.EQUALS =>
          true // We don't care about this since there's only one bound value.
        case _ =>
          return None
      }

    // reduce predicate to constants to compute bounds
    val (leftLiteral, rightLiteral) = reduceTimeExpression(timePred, rexBuilder, config)

    if (leftLiteral.isEmpty || rightLiteral.isEmpty) {
      return None
    }

    // compute boundary
    val tmpTimeOffset: Long = if (timePred.leftInputOnLeftSide) {
      rightLiteral.get - leftLiteral.get
    } else {
      leftLiteral.get - rightLiteral.get
    }
    val boundary = timePred.pred.getKind match {
      case SqlKind.LESS_THAN if timePred.leftInputOnLeftSide =>
        tmpTimeOffset - 1
      case SqlKind.LESS_THAN if !timePred.leftInputOnLeftSide =>
        tmpTimeOffset + 1
      case SqlKind.GREATER_THAN if timePred.leftInputOnLeftSide =>
        tmpTimeOffset + 1
      case SqlKind.GREATER_THAN if !timePred.leftInputOnLeftSide =>
        tmpTimeOffset - 1
      case _ =>
        tmpTimeOffset
    }

    Some(WindowBound(boundary, isLeftLowerBound))
  }

  /**
    * Replaces the time attributes on both sides of a time predicate by a zero literal and
    * reduces the expressions on both sides to a long literal.
    *
    * @param timePred   The time predicate which both sides are reduced.
    * @param rexBuilder A RexBuilder
    * @param config     A TableConfig.
    * @return The values of the reduced literals on both sides of the time comparison predicate.
    */
  private def reduceTimeExpression(
      timePred: TimePredicate,
      rexBuilder: RexBuilder,
      config: TableConfig): (Option[Long], Option[Long]) = {

    /**
      * Replace the time attribute by zero literal.
      */
    def replaceTimeFieldWithLiteral(expr: RexNode): RexNode = {
      expr match {
        case c: RexCall if RelTimeIndicatorConverter.isMaterializationCall(c) =>
          // replace with timestamp
          rexBuilder.makeZeroLiteral(expr.getType)
        case c: RexCall =>
          // replace in call operands
          val newOps = c.operands.map(replaceTimeFieldWithLiteral)
          rexBuilder.makeCall(c.getType, c.getOperator, newOps)
        case i: RexInputRef if FlinkTypeFactory.isTimeIndicatorType(i.getType) =>
          // replace with timestamp
          rexBuilder.makeZeroLiteral(expr.getType)
        case _ => expr
      }
    }

    val leftSide = timePred.pred.operands.get(0)
    val rightSide = timePred.pred.operands.get(1)

    val leftSideWithLiteral = replaceTimeFieldWithLiteral(leftSide)
    val rightSideWithLiteral = replaceTimeFieldWithLiteral(rightSide)

    // reduce expression to literal
     val exprReducer = new ExpressionReducer(config, allowChangeNullability = true)
    val originList = new util.ArrayList[RexNode]()
    originList.add(leftSideWithLiteral)
    originList.add(rightSideWithLiteral)
     val reduceList = new util.ArrayList[RexNode]()
     exprReducer.reduce(rexBuilder, originList, reduceList)

    // extract bounds from reduced literal
    val literals = reduceList.map {
      case literal: RexLiteral =>
        Some(literal.getValue2.asInstanceOf[Long])
      case _ =>
        None
    }

    (literals.head, literals(1))
  }

}
