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
package org.apache.flink.table.plan.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.table.api.{PlannerConfigOptions, TableConfig, TableException}
import org.apache.flink.table.plan.nodes.calcite.{ConstantRankRange, ConstantRankRangeWithoutEnd, RankRange, VariableRankRange}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.core.{AggregateCall, JoinInfo}
import org.apache.calcite.rel.{RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.{SqlExplainLevel, SqlKind}
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.mapping.IntPair

import java.io.{PrintWriter, StringWriter}
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

object FlinkRelOptUtil {

  /**
    * Converts a relational expression to a string.
    * This is different from [[RelOptUtil]]#toString on two points:
    * 1. Generated string by this method is in a tree style
    * 2. Generated string by this method may have more information about RelNode, such as
    * RelNode id, retractionTraits.
    *
    * @param rel                the RelNode to convert
    * @param detailLevel        detailLevel defines detail levels for EXPLAIN PLAN.
    * @param withIdPrefix       whether including ID of RelNode as prefix
    * @param withRetractTraits  whether including Retraction Traits of RelNode (only apply to
    * StreamPhysicalRel node at present)
    * @param withRowType        whether including output rowType
    * @return explain plan of RelNode
    */
  def toString(
      rel: RelNode,
      detailLevel: SqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withIdPrefix: Boolean = false,
      withRetractTraits: Boolean = false,
      withRowType: Boolean = false): String = {
    if (rel == null) {
      return null
    }
    val sw = new StringWriter
    val planWriter = new RelTreeWriterImpl(
      new PrintWriter(sw),
      detailLevel,
      withIdPrefix,
      withRetractTraits,
      withRowType)
    rel.explain(planWriter)
    sw.toString
  }


  /**
    * Get unique field name based on existed `allFieldNames` collection.
    * NOTES: the new unique field name will be added to existed `allFieldNames` collection.
    */
  def buildUniqueFieldName(
      allFieldNames: util.Set[String],
      toAddFieldName: String): String = {
    var name: String = toAddFieldName
    var i: Int = 0
    while (allFieldNames.contains(name)) {
      name = toAddFieldName + "_" + i
      i += 1
    }
    allFieldNames.add(name)
    name
  }

  /**
    * Returns indices of group functions.
    */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /**
    * Returns whether any of the aggregates are accurate DISTINCT.
    *
    * @return Whether any of the aggregates are accurate DISTINCT
    */
  def containsAccurateDistinctCall(aggCalls: Seq[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
    * Returns limit start value (never null).
    */
  def getLimitStart(offset: RexNode): Long = if (offset != null) RexLiteral.intValue(offset) else 0L

  /**
    * Returns limit end value (never null).
    */
  def getLimitEnd(offset: RexNode, fetch: RexNode): Long = {
    if (fetch != null) {
      getLimitStart(offset) + RexLiteral.intValue(fetch)
    } else {
      // TODO return Long.MaxValue when providing FlinkRelMdRowCount on Sort ?
      Integer.MAX_VALUE
    }
  }

  /**
    * Converts [[Direction]] to [[Order]].
    */
  def directionToOrder(direction: Direction): Order = {
    direction match {
      case Direction.ASCENDING | Direction.STRICTLY_ASCENDING => Order.ASCENDING
      case Direction.DESCENDING | Direction.STRICTLY_DESCENDING => Order.DESCENDING
      case _ => throw new IllegalArgumentException("Unsupported direction.")
    }
  }

  /**
    * Gets sort key indices, sort orders and null directions from given field collations.
    */
  def getSortKeysAndOrders(
      fieldCollations: Seq[RelFieldCollation]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val fieldMappingDirections = fieldCollations.map {
      c => (c.getFieldIndex, directionToOrder(c.getDirection))
    }
    val keys = fieldMappingDirections.map(_._1)
    val orders = fieldMappingDirections.map(_._2 == Order.ASCENDING)
    val nullsIsLast = fieldCollations.map(_.nullDirection).map {
      case RelFieldCollation.NullDirection.LAST => true
      case RelFieldCollation.NullDirection.FIRST => false
      case RelFieldCollation.NullDirection.UNSPECIFIED =>
        throw new TableException(s"Do not support UNSPECIFIED for null order.")
    }.toArray

    deduplicateSortKeys(keys.toArray, orders.toArray, nullsIsLast)
  }

  /**
    * Removes duplicate sort keys.
    */
  private def deduplicateSortKeys(
      keys: Array[Int],
      orders: Array[Boolean],
      nullsIsLast: Array[Boolean]): (Array[Int], Array[Boolean], Array[Boolean]) = {
    val keySet = new mutable.HashSet[Int]
    val keyBuffer = new mutable.ArrayBuffer[Int]
    val orderBuffer = new mutable.ArrayBuffer[Boolean]
    val nullsIsLastBuffer = new mutable.ArrayBuffer[Boolean]
    for (i <- keys.indices) {
      if (keySet.add(keys(i))) {
        keyBuffer += keys(i)
        orderBuffer += orders(i)
        nullsIsLastBuffer += nullsIsLast(i)
      }
    }
    (keyBuffer.toArray, orderBuffer.toArray, nullsIsLastBuffer.toArray)
  }

  /**
    * Check and get join left and right keys.
    */
  def checkAndGetJoinKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmptyKey: Boolean = false): (Array[Int], Array[Int]) = {
    // get the equality keys
    val leftKeys = mutable.ArrayBuffer.empty[Int]
    val rightKeys = mutable.ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmptyKey) {
        (leftKeys.toArray, rightKeys.toArray)
      } else {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tleft: ${left.toString}\n\tright: ${right.toString}\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach { pair =>
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys += pair.source
          rightKeys += pair.target
        } else {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}\n" +
              "please re-check the join statement.")
        }
      }
      (leftKeys.toArray, rightKeys.toArray)
    }
  }

  /**
    * Creates a [[JoinInfo]] by analyzing a condition.
    *
    * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]],
    * the only difference is that the methods could return `filterNulls`.
    */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining = RelOptUtil.splitJoinCondition(
      left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

  private[this] case class LimitPredicate(rankOnLeftSide: Boolean, pred: RexCall)

  private[this] sealed trait Boundary

  private[this] case class LowerBoundary(lower: Long) extends Boundary

  private[this] case class UpperBoundary(upper: Long) extends Boundary

  private[this] case class BothBoundary(lower: Long, upper: Long) extends Boundary

  private[this] case class InputRefBoundary(inputFieldIndex: Int) extends Boundary

  private[this] sealed trait BoundDefine

  private[this] object Lower extends BoundDefine // defined lower bound
  private[this] object Upper extends BoundDefine // defined upper bound
  private[this] object Both extends BoundDefine // defined lower and uppper bound

  /**
    * Extracts the TopN offset and fetch bounds from a predicate.
    *
    * @param  predicate           predicate
    * @param  rankFieldIndex      the index of rank field
    * @param  rexBuilder          RexBuilder
    * @param  config              TableConfig
    * @return A Tuple2 of extracted rank range and remaining predicates.
    */
  def extractRankRange(
      predicate: RexNode,
      rankFieldIndex: Int,
      rexBuilder: RexBuilder,
      config: TableConfig): (Option[RankRange], Option[RexNode]) = {

    // Converts the condition to conjunctive normal form (CNF)
    val cnfNodeCount = config.getConf.getInteger(PlannerConfigOptions.SQL_OPTIMIZER_CNF_NODES_LIMIT)
    val cnfCondition = FlinkRexUtil.toCnf(rexBuilder, cnfNodeCount, predicate)

    // split the condition into sort limit condition and other condition
    val (limitPreds: Seq[LimitPredicate], otherPreds: Seq[RexNode]) = cnfCondition match {
      case c: RexCall if c.getKind == SqlKind.AND =>
        c.getOperands
          .map(identifyLimitPredicate(_, rankFieldIndex))
          .foldLeft((Seq[LimitPredicate](), Seq[RexNode]())) {
            (preds, analyzed) =>
              analyzed match {
                case Left(limitPred) => (preds._1 :+ limitPred, preds._2)
                case Right(otherPred) => (preds._1, preds._2 :+ otherPred)
              }
          }
      case rex: RexNode =>
        identifyLimitPredicate(rex, rankFieldIndex) match {
          case Left(limitPred) => (Seq(limitPred), Seq())
          case Right(otherPred) => (Seq(), Seq(otherPred))
        }
      case _ =>
        return (None, Some(predicate))
    }

    if (limitPreds.isEmpty) {
      // no valid TopN bounds.
      return (None, Some(predicate))
    }

    val sortBounds = limitPreds.map(computeWindowBoundFromPredicate(_, rexBuilder, config))
    val rankRange = sortBounds match {
      case Seq(Some(LowerBoundary(x)), Some(UpperBoundary(y))) =>
        ConstantRankRange(x, y)
      case Seq(Some(UpperBoundary(x)), Some(LowerBoundary(y))) =>
        ConstantRankRange(y, x)
      case Seq(Some(LowerBoundary(x))) =>
        // only offset
        ConstantRankRangeWithoutEnd(x)
      case Seq(Some(UpperBoundary(x))) =>
        // rankStart starts from one
        ConstantRankRange(1, x)
      case Seq(Some(BothBoundary(x, y))) =>
        // nth rank
        ConstantRankRange(x, y)
      case Seq(Some(InputRefBoundary(x))) =>
        VariableRankRange(x)
      case _ =>
        // TopN requires at least one rank comparison predicate
        return (None, Some(predicate))
    }

    val remainCondition = otherPreds match {
      case Seq() => None
      case _ => Some(otherPreds.reduceLeft((l, r) => RelOptUtil.andJoinFilters(rexBuilder, l, r)))
    }

    (Some(rankRange), remainCondition)
  }

  /**
    * Analyzes a predicate and identifies whether it is a valid predicate for a TopN.
    * A valid TopN predicate is a comparison predicate (<, <=, =>, >) or equal predicate
    * that accesses rank fields of input rel node, the rank field reference must be on
    * one side of the condition alone.
    *
    * Examples:
    * - rank <= 10 => valid (Top 10)
    * - rank + 1 <= 10 => invalid: rank is not alone in the condition
    * - rank == 10 => valid (10th)
    * - rank <= rank + 2 => invalid: rank on same side
    *
    * @return Either a valid time predicate (Left) or a valid non-time predicate (Right)
    */
  private def identifyLimitPredicate(
      pred: RexNode,
      rankFieldIndex: Int): Either[LimitPredicate, RexNode] = pred match {
    case c: RexCall =>
      c.getKind match {
        case SqlKind.GREATER_THAN |
             SqlKind.GREATER_THAN_OR_EQUAL |
             SqlKind.LESS_THAN |
             SqlKind.LESS_THAN_OR_EQUAL |
             SqlKind.EQUALS =>

          val leftTerm = c.getOperands.head
          val rightTerm = c.getOperands.last

          if (isRankFieldRef(leftTerm, rankFieldIndex) &&
            !accessesRankField(rightTerm, rankFieldIndex)) {
            Left(LimitPredicate(rankOnLeftSide = true, c))
          } else if (isRankFieldRef(rightTerm, rankFieldIndex) &&
            !accessesRankField(leftTerm, rankFieldIndex)) {
            Left(LimitPredicate(rankOnLeftSide = false, c))
          } else {
            Right(pred)
          }

        // not a comparison predicate.
        case _ => Right(pred)
      }
    case _ => Right(pred)
  }

  // checks if the expression is the rank field reference
  def isRankFieldRef(expr: RexNode, rankFieldIndex: Int): Boolean = expr match {
    case i: RexInputRef => i.getIndex == rankFieldIndex
    case _ => false
  }

  /**
    * Checks if an expression accesses a rank field.
    *
    * @param expr The expression to check.
    * @param rankFieldIndex The rank field index.
    * @return True, if the expression accesses a time attribute. False otherwise.
    */
  def accessesRankField(expr: RexNode, rankFieldIndex: Int): Boolean = expr match {
    case i: RexInputRef => i.getIndex == rankFieldIndex
    case c: RexCall => c.operands.exists(accessesRankField(_, rankFieldIndex))
    case _ => false
  }

  /**
    * Computes the absolute bound on the left operand of a comparison expression and
    * whether the bound is an upper or lower bound.
    *
    * @return sort boundary (lower boundary, upper boundary)
    */
  private def computeWindowBoundFromPredicate(
      limitPred: LimitPredicate,
      rexBuilder: RexBuilder,
      config: TableConfig): Option[Boundary] = {

    val bound: BoundDefine = limitPred.pred.getKind match {
      case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL if limitPred.rankOnLeftSide =>
        Lower
      case SqlKind.GREATER_THAN | SqlKind.GREATER_THAN_OR_EQUAL if !limitPred.rankOnLeftSide =>
        Upper
      case SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL if limitPred.rankOnLeftSide =>
        Upper
      case SqlKind.LESS_THAN | SqlKind.LESS_THAN_OR_EQUAL if !limitPred.rankOnLeftSide =>
        Lower
      case SqlKind.EQUALS =>
        Both
    }

    val predExpression = if (limitPred.rankOnLeftSide) {
      limitPred.pred.operands.get(1)
    } else {
      limitPred.pred.operands.get(0)
    }

    (predExpression, bound) match {
      case (r: RexInputRef, Upper | Both) => Some(InputRefBoundary(r.getIndex))
      case (_: RexInputRef, Lower) => None
      case _ =>
        // reduce predicate to constants to compute bounds
        val literal = reduceComparisonPredicate(limitPred, rexBuilder, config)
        if (literal.isEmpty) {
          None
        } else {
          // compute boundary
          val tmpBoundary: Long = literal.get
          val boundary = limitPred.pred.getKind match {
            case SqlKind.LESS_THAN if limitPred.rankOnLeftSide =>
              tmpBoundary - 1
            case SqlKind.LESS_THAN =>
              tmpBoundary + 1
            case SqlKind.GREATER_THAN if limitPred.rankOnLeftSide =>
              tmpBoundary + 1
            case SqlKind.GREATER_THAN =>
              tmpBoundary - 1
            case _ =>
              tmpBoundary
          }
          bound match {
            case Lower => Some(LowerBoundary(boundary))
            case Upper => Some(UpperBoundary(boundary))
            case Both => Some(BothBoundary(boundary, boundary))
          }
        }
    }
  }

  /**
    * Replaces the rank aggregate reference on of a predicate by a zero literal and
    * reduces the expressions on both sides to a long literal.
    *
    * @param limitPred The limit predicate which both sides are reduced.
    * @param rexBuilder A RexBuilder
    * @param config A TableConfig.
    * @return The values of the reduced literals on both sides of the comparison predicate.
    */
  private def reduceComparisonPredicate(
      limitPred: LimitPredicate,
      rexBuilder: RexBuilder,
      config: TableConfig): Option[Long] = {

    val expression = if (limitPred.rankOnLeftSide) {
      limitPred.pred.operands.get(1)
    } else {
      limitPred.pred.operands.get(0)
    }

    if (!RexUtil.isConstant(expression)) {
      return None
    }

    // TODO reduce expression after ExpressionReducer introduced
    // reduce expression to literal
    // val exprReducer = new ExpressionReducer(config)
    // val originList = new util.ArrayList[RexNode]()
    // originList.add(expression)
    // val reduceList = new util.ArrayList[RexNode]()
    // exprReducer.reduce(rexBuilder, originList, reduceList)

    // extract bounds from reduced literal
    val literals = Array(expression).map {
      case literal: RexLiteral => Some(literal.getValue2.asInstanceOf[Long])
      case _ => None
    }

    literals.head
  }
}
