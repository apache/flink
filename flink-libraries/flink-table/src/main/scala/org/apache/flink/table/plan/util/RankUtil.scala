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

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.table.api.dataview.Order
import org.apache.flink.table.api.types.{DataTypes, RowType, TypeConverters}
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableException}
import org.apache.flink.table.codegen._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.plan.nodes.physical.stream.StreamExecRank
import org.apache.flink.table.plan.rules.physical.stream.StreamExecRetractionRules
import org.apache.flink.table.plan.schema.BaseRowSchema
import org.apache.flink.table.runtime.aggregate.SorterHelper
import org.apache.flink.table.runtime.{BinaryRowKeySelector, NullBinaryRowKeySelector}
import org.apache.flink.table.typeutils.BaseRowTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelOptUtil}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.validate.SqlMonotonicity

import java.util

import scala.collection.JavaConversions._

/**
  * An util class to optimize and generate TopN operators.
  */
object RankUtil {

  case class LimitPredicate(rankOnLeftSide: Boolean, pred: RexCall)

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
    val cnfCondition = FlinkRexUtil.toCnf(rexBuilder,
      config.getConf.getInteger(TableConfigOptions.SQL_OPTIMIZER_CNF_NODES_LIMIT),
      predicate)

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
      case Seq() =>
        None
      case _ =>
        Some(otherPreds.reduceLeft((l, r) => RelOptUtil.andJoinFilters(rexBuilder, l, r)))
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


  sealed trait Boundary

  case class LowerBoundary(lower: Long) extends Boundary

  case class UpperBoundary(upper: Long) extends Boundary

  case class BothBoundary(lower: Long, upper: Long) extends Boundary

  case class InputRefBoundary(inputFieldIndex: Int) extends Boundary

  sealed trait BoundDefine

  object Lower extends BoundDefine // defined lower bound
  object Upper extends BoundDefine // defined upper bound
  object Both extends BoundDefine // defined lower and uppper bound

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

    // reduce expression to literal
    val exprReducer = new ExpressionReducer(config)
    val originList = new util.ArrayList[RexNode]()
    originList.add(expression)
    val reduceList = new util.ArrayList[RexNode]()
    exprReducer.reduce(rexBuilder, originList, reduceList)

    // extract bounds from reduced literal
    val literals = reduceList.map {
      case literal: RexLiteral => Some(literal.getValue2.asInstanceOf[Long])
      case _ => None
    }

    literals.head
  }

  def getOrderFromFieldCollation(field: RelFieldCollation): Order = {
    field.getDirection match {
      case RelFieldCollation.Direction.ASCENDING
           | RelFieldCollation.Direction.STRICTLY_ASCENDING =>
        Order.ASCENDING

      case RelFieldCollation.Direction.DESCENDING
           | RelFieldCollation.Direction.STRICTLY_DESCENDING =>
        Order.DESCENDING

      case _ =>
        //Shouldn't happen
        throw new TableException(
          "Couldn't get correct sort field direction. Shouldn't happen here.")
    }
  }

  def createSortKeyTypeAndSorter(
      inputSchema: BaseRowSchema,
      fieldCollations: Seq[RelFieldCollation]): (BaseRowTypeInfo, GeneratedSorter) = {

    val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)

    val inputFieldTypes = inputSchema.fieldTypeInfos
    val fieldTypes = sortFields.map(inputFieldTypes(_))
    val sortKeyTypeInfo = new BaseRowTypeInfo(fieldTypes: _*)

    val logicalKeyFields = fieldTypes.indices.toArray

    val sorter = SorterHelper.createSorter(
      sortKeyTypeInfo.getFieldTypes.map(TypeConverters.createInternalTypeFromTypeInfo),
      logicalKeyFields,
      sortDirections,
      nullsIsLast)

    (sortKeyTypeInfo, sorter)
  }

  def createSortKeySelector(
      fieldCollations: Seq[RelFieldCollation],
      inputSchema: BaseRowSchema): KeySelector[BaseRow, BaseRow] = {

    val sortFields = fieldCollations.map(_.getFieldIndex).toArray
    if (sortFields.isEmpty) {
      new NullBinaryRowKeySelector
    } else {
      new BinaryRowKeySelector(sortFields, inputSchema.typeInfo())
    }
  }

  def getUnarySortKeyExtractor(
      fieldCollations: Seq[RelFieldCollation],
      inputSchema: BaseRowSchema): GeneratedFieldExtractor = {

    val sortFields = fieldCollations.map(_.getFieldIndex).toArray
    if (sortFields.length != 1) {
      throw new TableException("[rank util] This shouldn't happen. Please file an issue.")
    }

    val inputType: BaseRowTypeInfo = inputSchema.typeInfo()

    FieldAccessCodeGenerator.generateRowFieldExtractor(
      CodeGeneratorContext.apply(new TableConfig, supportReference = false),
      "SimpleFieldExtractor",
      TypeConverters.createInternalTypeFromTypeInfo(inputType).asInstanceOf[RowType],
      sortFields.head)
  }

  def createRowKeyType(
      primaryKeys: Array[Int],
      inputSchema: BaseRowSchema): BaseRowTypeInfo = {

    val fieldTypes = primaryKeys.map(inputSchema.fieldTypeInfos(_))
    new BaseRowTypeInfo(fieldTypes: _*)
  }

  def createKeySelector(
      keys: Array[Int],
      inputSchema: BaseRowSchema): KeySelector[BaseRow, BaseRow] = {
    if (keys.isEmpty) {
      new NullBinaryRowKeySelector
    } else {
      new BinaryRowKeySelector(keys, inputSchema.typeInfo())
    }
  }

  def analyzeRankStrategy(
      cluster: RelOptCluster,
      tableConfig: TableConfig,
      rank: StreamExecRank,
      sortCollation: RelCollation): RankStrategy = {

    val rankInput = rank.getInput
    val fieldCollations = sortCollation.getFieldCollations

    val mono = cluster.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
      .getRelModifiedMonotonicity(rankInput)

    val isMonotonic = if (mono == null) {
      false
    } else {
      if (fieldCollations.isEmpty) {
        false
      } else {
        fieldCollations.forall(collation => {
          val fieldMono = mono.fieldMonotonicities(collation.getFieldIndex)
          val direction = collation.direction
          if ((fieldMono == SqlMonotonicity.DECREASING
            || fieldMono == SqlMonotonicity.STRICTLY_DECREASING)
            && direction == Direction.ASCENDING) {
            // sort field is ascending and its mono is decreasing when arriving rank node
            true
          } else if ((fieldMono == SqlMonotonicity.INCREASING
            || fieldMono == SqlMonotonicity.STRICTLY_INCREASING)
            && direction == Direction.DESCENDING) {
            // sort field is descending and its mono is increasing when arriving rank node
            true
          } else if (fieldMono == SqlMonotonicity.CONSTANT) {
            // sort key is a grouping key of upstream agg, it is monotonic
            true
          } else {
            false
          }
        })
      }
    }

    val isUpdateStream = !UpdatingPlanChecker.isAppendOnly(rankInput)
    val partitionKey = rank.partitionKey

    if (isUpdateStream) {
      val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(rankInput)
      val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(rankInput)
      if (inputIsAccRetract || uniqueKeys == null || uniqueKeys.isEmpty
        // unique key should contains partition key
        || !uniqueKeys.exists(k => k.contains(partitionKey))) {
        // input is AccRetract or extract the unique keys failed,
        // and we fall back to using retract rank
        RetractRank
      } else {
        if (isMonotonic) {
          //FIXME choose a set of primary key
          UpdateFastRank(uniqueKeys.iterator().next().toArray)
        } else {
          if (fieldCollations.length == 1) {
            // single sort key in update stream scenario (no monotonic)
            // we can utilize unary rank function to speed up processing
            UnaryUpdateRank(uniqueKeys.iterator().next().toArray)
          } else {
            // no other choices, have to use retract rank
            RetractRank
          }
        }
      }
    } else {
      AppendFastRank
    }
  }

  sealed trait RankStrategy

  case object AppendFastRank extends RankStrategy

  case object RetractRank extends RankStrategy

  case class UpdateFastRank(primaryKeys: Array[Int]) extends RankStrategy {
    override def toString: String = {
      "UpdateFastRank" + primaryKeys.mkString("[", ",", "]")
    }
  }

  case class UnaryUpdateRank(primaryKeys: Array[Int]) extends RankStrategy {
    override def toString: String = {
      "UnaryUpdateRank" + primaryKeys.mkString("[", ",", "]")
    }
  }

}
