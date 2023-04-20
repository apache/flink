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
package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.JDouble
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalGroupAggregateBase, BatchPhysicalLocalHashWindowAggregate, BatchPhysicalLocalSortWindowAggregate, BatchPhysicalWindowAggregateBase}
import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.AggregateUtil

import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rex._
import org.apache.calcite.sql.{SqlKind, SqlOperator}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._

import scala.collection.JavaConversions._

/**
 * Estimates selectivity of rows meeting an agg-call predicate on an Aggregate.
 *
 * A filter predicate on an Aggregate may contain two parts: one is on group by columns, another is
 * on aggregate call's result. The first part is handled by [[SelectivityEstimator]], the second
 * part is handled by this Estimator.
 *
 * @param agg
 *   aggregate node
 * @param mq
 *   Metadata query
 */
class AggCallSelectivityEstimator(agg: RelNode, mq: FlinkRelMetadataQuery)
  extends RexVisitorImpl[Option[Double]](true) {

  private val rexBuilder = agg.getCluster.getRexBuilder
  // create SelectivityEstimator instance to use its default selectivity values
  private val se = new SelectivityEstimator(agg, mq)
  private[flink] val defaultAggCallSelectivity = Some(0.01d)

  /** Gets AggregateCall from aggregate node */
  def getSupportedAggCall(outputIdx: Int): Option[AggregateCall] = {
    val (fullGrouping, aggCalls) = agg match {
      case rel: Aggregate =>
        val (auxGroupSet, otherAggCalls) = AggregateUtil.checkAndSplitAggCalls(rel)
        (rel.getGroupSet.toArray ++ auxGroupSet, otherAggCalls)
      case rel: BatchPhysicalGroupAggregateBase =>
        (rel.grouping ++ rel.auxGrouping, rel.getAggCallList)
      case rel: BatchPhysicalLocalHashWindowAggregate =>
        val fullGrouping = rel.grouping ++ Array(rel.inputTimeFieldIndex) ++ rel.auxGrouping
        (fullGrouping, rel.getAggCallList)
      case rel: BatchPhysicalLocalSortWindowAggregate =>
        val fullGrouping = rel.grouping ++ Array(rel.inputTimeFieldIndex) ++ rel.auxGrouping
        (fullGrouping, rel.getAggCallList)
      case rel: BatchPhysicalWindowAggregateBase =>
        (rel.grouping ++ rel.auxGrouping, rel.getAggCallList)
      case _ => throw new IllegalArgumentException(s"Cannot handle ${agg.getRelTypeName}!")
    }
    require(outputIdx >= fullGrouping.length)
    val aggCallIdx = outputIdx - fullGrouping.length
    val aggCall = if (aggCallIdx < aggCalls.length) aggCalls.get(aggCallIdx) else null
    Option(aggCall).filter(isSupportedAggCall)
  }

  /** Returns whether the given aggCall is supported now TODO supports more */
  def isSupportedAggCall(aggCall: AggregateCall): Boolean = {
    aggCall.getAggregation.getKind match {
      case SqlKind.SUM | SqlKind.MAX | SqlKind.MIN | SqlKind.AVG => true
      case SqlKind.COUNT => aggCall.getArgList.size() == 1
      case _ => false
    }
  }

  /** Gets aggCall's interval through its argument's interval. */
  def getAggCallInterval(aggCall: AggregateCall): ValueInterval = {
    val aggInput = agg.getInput(0)

    // assumes that the data is uniform distribution
    def getRowCntPerGroup: Option[Double] = {
      val inputRowCnt = mq.getRowCount(aggInput)
      if (inputRowCnt == null) {
        return None
      }
      val aggRowCnt = mq.getRowCount(agg)
      if (aggRowCnt == null) {
        return None
      }
      Some(inputRowCnt / aggRowCnt)
    }

    if (aggCall.getAggregation.getKind == SqlKind.COUNT) {
      return getRowCntPerGroup match {
        case Some(rowCntPerGroup) =>
          // assumes the min count is half of the average count per group,
          // the max count is double of the average count per group
          val lower = math.max(rowCntPerGroup / 2, 1)
          val upper = rowCntPerGroup * 2
          ValueInterval(lower, upper, includeLower = true, includeUpper = true)
        case _ => null
      }
    }

    val argInterval = mq.getColumnInterval(aggInput, aggCall.getArgList.head)
    argInterval match {
      case null => null
      case ValueInterval.infinite => ValueInterval.infinite
      case ValueInterval.empty => ValueInterval.empty
      case _ =>
        val (min, includeMin) = argInterval match {
          case hasLower: WithLower =>
            (SelectivityEstimator.comparableToDouble(hasLower.lower), hasLower.includeLower)
          case _ => (null, true)
        }
        val (max, includeMax) = argInterval match {
          case hasUpper: WithUpper =>
            (SelectivityEstimator.comparableToDouble(hasUpper.upper), hasUpper.includeUpper)
          case _ => (null, true)
        }

        def getAggCallValue(v: JDouble): JDouble = {
          if (v == null) {
            return null
          }
          aggCall.getAggregation.getKind match {
            case SqlKind.MAX | SqlKind.MIN | SqlKind.AVG => v
            case SqlKind.SUM =>
              getRowCntPerGroup match {
                case Some(rowCntPerGroup) =>
                  // assume uniform distribution now
                  v * rowCntPerGroup
                case _ => null
              }
          }
        }

        ValueInterval(getAggCallValue(min), getAggCallValue(max), includeMin, includeMax)
    }
  }

  /**
   * Returns a percentage of rows meeting a filter predicate on aggregate.
   *
   * @param predicate
   *   predicate whose selectivity is to be estimated against aggregate calls.
   * @return
   *   estimated selectivity (between 0.0 and 1.0), or None if no reliable estimate can be
   *   determined.
   */
  def evaluate(predicate: RexNode): Option[Double] = {
    try {
      if (predicate == null) {
        Some(1.0)
      } else {
        val rexSimplify =
          new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
        val simplifiedPredicate =
          rexSimplify.simplifyUnknownAs(predicate, RexUnknownAs.falseIf(true))
        if (simplifiedPredicate.isAlwaysTrue) {
          Some(1.0)
        } else if (simplifiedPredicate.isAlwaysFalse) {
          Some(0.0)
        } else {
          simplifiedPredicate.accept(this)
        }
      }
    } catch {
      // if found unsupported operations, fallback
      case _: Throwable => None
    }
  }

  override def visitCall(call: RexCall): Option[Double] = {
    val operands = call.getOperands
    call.getOperator match {
      case AND =>
        val selectivity = operands.map(estimateOperand)
        Some(selectivity.product)

      case OR =>
        val selectivity = operands.map(estimateOperand)
        Some(math.min(1.0, selectivity.sum - selectivity.product))

      case NOT =>
        val selectivity = estimateOperand(operands.head)
        Some(1.0 - selectivity)

      case _ =>
        estimateSinglePredicate(call)
    }
  }

  def estimateOperand(operand: RexNode): Double = {
    val subSelectivity = operand.accept(this)
    if (subSelectivity != null) subSelectivity.getOrElse(1.0) else 1.0
  }

  /**
   * Returns a percentage of rows meeting a single condition in Filter node.
   *
   * @param singlePredicate
   *   predicate whose selectivity is to be estimated against aggregate calls.
   * @return
   *   an optional double value to show the percentage of rows meeting a given condition. It returns
   *   None if the condition is not supported.
   */
  private def estimateSinglePredicate(singlePredicate: RexCall): Option[Double] = {
    val operands = singlePredicate.getOperands
    singlePredicate.getOperator match {
      case EQUALS =>
        estimateComparison(EQUALS, operands.head, operands.last)

      case NOT_EQUALS =>
        val selectivity = estimateComparison(EQUALS, operands.head, operands.last)
        Some(1.0 - selectivity.getOrElse(1.0))

      case GREATER_THAN =>
        estimateComparison(GREATER_THAN, operands.head, operands.last)

      case GREATER_THAN_OR_EQUAL =>
        estimateComparison(GREATER_THAN_OR_EQUAL, operands.head, operands.last)

      case LESS_THAN =>
        estimateComparison(LESS_THAN, operands.head, operands.last)

      case LESS_THAN_OR_EQUAL =>
        estimateComparison(LESS_THAN_OR_EQUAL, operands.head, operands.last)

      case RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC =>
        Option(RelMdUtil.getSelectivityValue(singlePredicate))

      case _ =>
        se.defaultSelectivity
    }
  }

  /**
   * Returns a percentage of rows meeting a binary comparison expression containing two columns.
   *
   * @param op
   *   a binary comparison operator, including =, <=>, <, <=, >, >=
   * @param left
   *   the left RexInputRef
   * @param right
   *   the right RexInputRef
   * @return
   *   an optional double value to show the percentage of rows meeting a given condition. It returns
   *   None if no statistics collected for a given column.
   */
  private def estimateComparison(op: SqlOperator, left: RexNode, right: RexNode): Option[Double] = {
    // if we can't handle some cases, uses SelectivityEstimator's default value
    // (consistent with normal case).
    // otherwise uses defaultAggCallSelectivity as default value.
    if (
      !SelectivityEstimator.isSupportedComparisonType(left.getType) ||
      !SelectivityEstimator.isSupportedComparisonType(right.getType)
    ) {
      val default = op match {
        case EQUALS => se.defaultEqualsSelectivity
        case _ => se.defaultComparisonSelectivity
      }
      return default
    }

    op match {
      case EQUALS =>
        (left, right) match {
          case (i: RexInputRef, l: RexLiteral) => estimateEquals(i, l)
          case (l: RexLiteral, i: RexInputRef) => estimateEquals(i, l)
          case _ => se.defaultEqualsSelectivity
        }
      case LESS_THAN =>
        (left, right) match {
          case (i: RexInputRef, l: RexLiteral) => estimateComparison(LESS_THAN, i, l)
          case (l: RexLiteral, i: RexInputRef) => estimateComparison(GREATER_THAN, i, l)
          case _ => se.defaultComparisonSelectivity
        }
      case LESS_THAN_OR_EQUAL =>
        (left, right) match {
          case (i: RexInputRef, l: RexLiteral) => estimateComparison(LESS_THAN_OR_EQUAL, i, l)
          case (l: RexLiteral, i: RexInputRef) => estimateComparison(GREATER_THAN_OR_EQUAL, i, l)
          case _ => se.defaultComparisonSelectivity
        }
      case GREATER_THAN =>
        (left, right) match {
          case (i: RexInputRef, l: RexLiteral) => estimateComparison(GREATER_THAN, i, l)
          case (l: RexLiteral, i: RexInputRef) => estimateComparison(LESS_THAN, i, l)
          case _ => se.defaultComparisonSelectivity
        }
      case GREATER_THAN_OR_EQUAL =>
        (left, right) match {
          case (i: RexInputRef, l: RexLiteral) => estimateComparison(GREATER_THAN_OR_EQUAL, i, l)
          case (l: RexLiteral, i: RexInputRef) => estimateComparison(LESS_THAN_OR_EQUAL, i, l)
          case _ => se.defaultComparisonSelectivity
        }
      case _ => se.defaultComparisonSelectivity
    }
  }

  /**
   * Returns a percentage of rows meeting an equality (=) expression. e.g. count(a) = 10
   *
   * @param inputRef
   *   a RexInputRef
   * @param literal
   *   a literal value (or constant)
   * @return
   *   an optional double value to show the percentage of rows meeting a given condition. It returns
   *   None if no statistics collected for a given column.
   */
  private def estimateEquals(inputRef: RexInputRef, literal: RexLiteral): Option[Double] = {
    if (literal.isNull) {
      return se.defaultIsNullSelectivity
    }

    val aggCall = getSupportedAggCall(inputRef.getIndex)
    if (!SelectivityEstimator.canConvertToNumericType(inputRef.getType) || aggCall.isEmpty) {
      return se.defaultEqualsSelectivity
    }
    val aggCallInterval = getAggCallInterval(aggCall.get)
    if (aggCallInterval == null) {
      return se.defaultEqualsSelectivity
    }
    val convertedInterval =
      SelectivityEstimator.convertValueInterval(aggCallInterval, inputRef.getType)
    convertedInterval match {
      case ValueInterval.infinite => se.defaultEqualsSelectivity
      case ValueInterval.empty =>
        // return defaultAggCallSelectivity instead of 0.0
        defaultAggCallSelectivity
      case i: FiniteValueInterval =>
        val min = SelectivityEstimator.comparableToDouble(i.lower)
        val max = SelectivityEstimator.comparableToDouble(i.upper)
        if (ValueInterval.contains(i, SelectivityEstimator.literalToComparable(literal))) {
          // the agg call interval is an estimated value, not a correct value.
          // if `1.0 / (max - min)` is too small, uses default value
          Some(math.max(defaultAggCallSelectivity.get, 1.0 / (max - min)))
        } else {
          defaultAggCallSelectivity
        }
      case _ => se.defaultEqualsSelectivity
    }
  }

  /**
   * Returns a percentage of rows meeting a binary comparison expression. e.g. sum(a) > 10
   *
   * @param op
   *   a binary comparison operator, including <, <=, >, >=
   * @param inputRef
   *   a RexInputRef
   * @param literal
   *   a literal value (or constant)
   * @return
   *   an optional double value to show the percentage of rows meeting a given condition. It returns
   *   None if no statistics collected for a given column.
   */
  private def estimateComparison(
      op: SqlOperator,
      inputRef: RexInputRef,
      literal: RexLiteral): Option[Double] = {
    if (literal.isNull) {
      throw new IllegalArgumentException("Numeric comparison does not support null literal here.")
    }
    val aggCall = getSupportedAggCall(inputRef.getIndex)
    if (SelectivityEstimator.canConvertToNumericType(inputRef.getType) && aggCall.isDefined) {
      estimateNumericComparison(op, aggCall.get, literal)
    } else {
      // TODO: It is difficult to support binary comparisons for non-numeric type
      // without advanced statistics like histogram.
      se.defaultComparisonSelectivity
    }
  }

  /**
   * Returns a percentage of rows meeting a binary numeric comparison expression. This method
   * evaluate expression for Numeric/Boolean/Date/Time/Timestamp columns.
   *
   * @param op
   *   a binary comparison operator, including <, <=, >, >=
   * @param aggCall
   *   an AggregateCall
   * @param literal
   *   a literal value (or constant)
   * @return
   *   an optional double value to show the percentage of rows meeting a given condition. It returns
   *   None if no statistics collected for a given column.
   */
  private def estimateNumericComparison(
      op: SqlOperator,
      aggCall: AggregateCall,
      literal: RexLiteral): Option[Double] = {
    val aggCallInterval = getAggCallInterval(aggCall)
    if (aggCallInterval == null) {
      return se.defaultComparisonSelectivity
    }

    aggCallInterval match {
      case ValueInterval.infinite => se.defaultComparisonSelectivity
      case ValueInterval.empty =>
        // return defaultAggCallSelectivity instead of 0.0
        defaultAggCallSelectivity
      case _ =>
        val (min, includeMin) = aggCallInterval match {
          case hasLower: WithLower =>
            (SelectivityEstimator.comparableToDouble(hasLower.lower), hasLower.includeLower)
          case _ => (null, true)
        }
        val (max, includeMax) = aggCallInterval match {
          case hasUpper: WithUpper =>
            (SelectivityEstimator.comparableToDouble(hasUpper.upper), hasUpper.includeUpper)
          case _ => (null, true)
        }
        val lit = SelectivityEstimator.literalToDouble(literal)
        val (noOverlap, completeOverlap) = op match {
          case LESS_THAN =>
            val noOverlap = SelectivityEstimator.greaterThanOrEqualTo(min, lit)
            val completeOverlap =
              if (includeMax) SelectivityEstimator.lessThan(max, lit)
              else SelectivityEstimator.lessThanOrEqualTo(max, lit)
            (noOverlap, completeOverlap)
          case LESS_THAN_OR_EQUAL =>
            val noOverlap =
              if (includeMin) SelectivityEstimator.greaterThan(min, lit)
              else SelectivityEstimator.greaterThanOrEqualTo(min, lit)
            val completeOverlap = SelectivityEstimator.lessThanOrEqualTo(max, lit)
            (noOverlap, completeOverlap)
          case GREATER_THAN =>
            val noOverlap = SelectivityEstimator.lessThanOrEqualTo(max, lit)
            val completeOverlap =
              if (includeMin) SelectivityEstimator.greaterThan(min, lit)
              else SelectivityEstimator.greaterThanOrEqualTo(min, lit)
            (noOverlap, completeOverlap)
          case GREATER_THAN_OR_EQUAL =>
            val noOverlap =
              if (includeMax) SelectivityEstimator.lessThan(max, lit)
              else SelectivityEstimator.lessThanOrEqualTo(max, lit)
            val completeOverlap = SelectivityEstimator.greaterThanOrEqualTo(min, lit)
            (noOverlap, completeOverlap)
        }

        val selectivity = if (noOverlap) {
          // return defaultAggCallSelectivity instead of 0.0
          defaultAggCallSelectivity.get
        } else if (completeOverlap) {
          // return 1 - defaultAggCallSelectivity instead of 1.0
          1.0 - defaultAggCallSelectivity.get
        } else if (min != null && max != null) {
          op match {
            case LESS_THAN | LESS_THAN_OR_EQUAL => (lit - min) / (max - min)
            case GREATER_THAN | GREATER_THAN_OR_EQUAL => (max - lit) / (max - min)
          }
        } else {
          se.defaultComparisonSelectivity.get
        }
        Some(selectivity)
    }
  }

}
