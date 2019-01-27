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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkRelFactories}
import org.apache.flink.table.plan.rules.logical.RewriteSelfJoinRule.{RankInfo, SequenceTracer}
import org.apache.flink.table.plan.util.{ConstantRankRange, RelDigestWriterImpl}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalFilter, LogicalProject}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules.{LoptMultiJoin, MultiJoin}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.{ImmutableBitSet, Pair}
import org.apache.calcite.util.mapping.Mappings

import com.google.common.collect.ImmutableList

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Rule used to promote an agg filter query, especially when we want to pick out table
  * records that match grouping max/min.
  *
  * <p>A typical query would be like:
  * <pre>
  * person table
  * schema: (
  *   id: int,
  *   age: smallint,
  *   name: varchar(128),
  *   height: smallint,
  *   sex: varchar(10))
  *
  * query:
  *   select * from person
  *   where age = (select max(age)
  *               from person p
  *               and p.height = person.height)
  * </pre>
  *
  * A classic query plan for above query would be like:
  * <pre>
  *          scan - - agg -
  *                        \
  *                        join - prj
  *                       /
  *          scan - - - -
  * </pre>
  * While after this rule, it will promotes to:
  * <pre>
  *          scan - rank - prj
  * </pre>
  * In production, the <b>scan</b> may be more complex, e.g. it can be a join of multiple tables
  * or an agg from some original table.
  *
  * <p>Caution that we do not make sure the join order is best after this promotion,
  * so a join reorder is needed later on to make the query more efficient.
  *
  * <p>This rule is only used for Hep planner.
  */
abstract class RewriteSelfJoinRule extends RelOptRule(
  operand(classOf[MultiJoin], any),
  FlinkRelFactories.FLINK_REL_BUILDER,
  "RewriteSelfJoinRule") {

  protected def isNonInnerJoinOrSelfJoin(
    multiJoin: LoptMultiJoin,
    factIdx: Int): Boolean = {
    multiJoin.isNullGenerating(factIdx) || multiJoin.getJoinRemovalFactor(factIdx) != null
  }

  /** Decide if there is an MultiJoin under the agg node. */
  protected def isMultiJoinMatched(rel: RelNode): Boolean = {
    getRealFactor(rel) match {
      case agg: LogicalAggregate =>
        val multiJoinFinder = new MultiJoinFinder
        agg.accept(multiJoinFinder)
        multiJoinFinder.getMultiJoin.nonEmpty
      case _ => true
    }
  }

  /** Decide if the factor is simple(scan with only calc), skip the agg node. */
  protected def isSimpleFactorMatched(rel: RelNode, mq: RelMetadataQuery): Boolean = {
    getRealFactor(rel) match {
      case _: LogicalAggregate => true
      case _ => mq.getTableOrigin(rel) != null
    }
  }

  /** Decide if the agg node is with single (Max/Min) agg call. */
  protected def isMaxMinAggMatched(rel: RelNode): Boolean = {
    getRealFactor(rel) match {
      case agg: LogicalAggregate =>
        // For rank, only cares about Max/Min agg correlate.
        // For simple cases of two tables self join, we do not have nested multi-join
        // under agg.
        val aggCallKind = agg.getAggCallList.head.getAggregation.getKind
        (aggCallKind == SqlKind.MAX || aggCallKind == SqlKind.MIN) &&
          agg.getAggCallList.size == 1
      case _ => true
    }
  }

  /** Get the aggregate filter in the outer MultiJoin, will return empty if found
    * multiple filters. */
  protected def getFactorSingleFilter(
    multiJoin: LoptMultiJoin,
    fieldsStart: Int): Option[RexNode] = {
    val filters = multiJoin.getJoinFilters.filter {
      f =>
        multiJoin.getFactorsRefByJoinFilter(f).cardinality == 2 &&
          isEqualsFilterWithInputRef(f, fieldsStart)
    }
    // Do not support empty or multi filters now.
    if (filters.lengthCompare(1) != 0) {
      Option.empty
    } else {
      Option(filters.head)
    }
  }

  /**
    * Append the group and agg column at the end of the project, which account to the
    * original group correlate and agg in the nested MultiJoin. We can do this cause the agg
    * is always the last factor in the outer MultiJoin.
    *
    * @param offsets Fields indices you wanna append, this is relative to the input node.
    * @param rowType The specified row type for the append input refs, just used to ensure the
    *                output type, especially the nullability.
    */
  protected def buildNewInputRefs(
    rexBuilder: RexBuilder,
    input: RelNode,
    offsets: ImmutableBitSet,
    rowType: RelDataType = null): ArrayBuffer[RexNode] = {
    val numFields = input.getRowType.getFieldCount
    val inputRefs = mutable.ArrayBuffer[RexNode]()
    val suffixRefs = mutable.ArrayBuffer[RexNode]()
    var offsetToType: Map[Integer, RelDataType] = null
    if (rowType != null) {
      offsetToType = offsets.zip(rowType.getFieldList.map(f => f.getType)).toMap
    }
    (0 until numFields).foreach {
      i =>
        val ref = RexInputRef.of(i, input.getRowType)
        inputRefs += ref
        offsets.filter(_ == i).foreach {
          offset =>
            val appendRef = rowType match {
              case null => ref
              case _ => rexBuilder.ensureType(offsetToType(offset), ref, false)
            }
            suffixRefs += appendRef
        }
    }
    require(offsets.cardinality == suffixRefs.size)
    // Assume group column is in front of agg column.
    inputRefs ++= suffixRefs
    inputRefs
  }

  /**
    * Compute the new field offset in the [[LoptMultiJoin]] instance.
    *
    * @param newFactors  new factors sequence in the multi-join.
    * @param factorIdx   factor id of the field.
    * @param fieldOffset field offset in the factor.
    */
  protected def newFieldOffset(
    multijoin: LoptMultiJoin,
    newFactors: ImmutableBitSet,
    factorIdx: Int,
    fieldOffset: Int): Int = {
    var startOffset = 0
    for (f <- newFactors) {
      if (factorIdx == f) {
        return startOffset + fieldOffset
      } else {
        startOffset += multijoin.getJoinFactor(f).getRowType.getFieldCount
      }
    }
    -1
  }

  /**
    * Get the correlate field offset in the ori table.
    */
  protected def relationFieldOffset(
    multiJoin: LoptMultiJoin,
    condition: RexNode,
    factorIdx: Int,
    refIndex: Int): Int = {
    require(condition.isInstanceOf[RexCall])
    val call = condition.asInstanceOf[RexCall]
    val op1 = call.getOperands.head
    val op2 = call.getOperands.last
    require(op1.isInstanceOf[RexInputRef])
    require(op2.isInstanceOf[RexInputRef])
    val idx1 = op1.asInstanceOf[RexInputRef].getIndex
    val idx2 = op2.asInstanceOf[RexInputRef].getIndex
    require(idx1 == refIndex || idx2 == refIndex)
    var startOffset = 0
    for (f <- 0 until factorIdx) {
      startOffset += multiJoin.getJoinFactor(f).getRowType.getFieldCount
    }

    if (idx1 == refIndex) {
      idx2 - startOffset
    } else {
      idx1 - startOffset
    }
  }

  /**
    * Decide if two [[LoptMultiJoin]] are equal with semanteme.
    * @param join1            [[LoptMultiJoin]] instance.
    * @param join2            [[LoptMultiJoin]] instance.
    * @param filtersToCompare filters of join1 to compare with join2 filters.
    * @return true if it is equals.
    */
  protected def multiJoinEq(
    join1: LoptMultiJoin,
    join2: LoptMultiJoin,
    filtersToCompare: List[RexNode]): Boolean = {
    val filters = join2.getJoinFilters
    if (filtersToCompare.lengthCompare(filters.size) != 0) {
      return false
    }
    val unusedFilters1: JList[RexNode] = new JArrayList[RexNode](filtersToCompare)
    val unusedFilters2: JList[RexNode] = new JArrayList[RexNode](filters)
    val itr1 = unusedFilters1.iterator
    while (itr1.hasNext) {
      val condition1 = itr1.next
      val factors1 = getFactors(condition1, join1)
      val itr2 = unusedFilters2.iterator
      var found = false
      while (itr2.hasNext && !found) {
        val condition2 = itr2.next
        val factors2 = getFactors(condition2, join2)

        require(factors1.length == 2 && factors2.length == 2)
        if (eqNode(factors1.head, factors2.last)
          && eqNode(factors1.last, factors2.head)
          || eqNode(factors1.head, factors2.head)
          && eqNode(factors1.last, factors2.last)) {
          itr1.remove()
          itr2.remove()
          found = true
        }
      }
    }
    unusedFilters1.isEmpty && unusedFilters2.isEmpty
  }

  protected def refFactors(f: RexNode, multiJoin: LoptMultiJoin): ImmutableBitSet = {
    val fieldRefBitmap = RelOptUtil.InputFinder.bits(f)
    val factorRefBitmap = ImmutableBitSet.builder
    fieldRefBitmap
      .map(field => multiJoin.findRef(field))
      .foreach(factor => factorRefBitmap.set(factor))
    factorRefBitmap.build()
  }

  private def getFactors(
    condition: RexNode,
    multiJoin: LoptMultiJoin): Array[RelNode] = {
    val ret = mutable.ArrayBuffer[RelNode]()
    ret ++= refFactors(condition, multiJoin)
      .map(factorIdx => multiJoin.getJoinFactor(factorIdx))
    ret.toArray
  }

  protected def eqNode(rel1: RelNode, rel2: RelNode): Boolean = {
    val digest1 = RelDigestWriterImpl.getDigest(getRealFactor(rel1))
    val digest2 = RelDigestWriterImpl.getDigest(getRealFactor(rel2))
    digest1.equals(digest2)
  }

  protected def getRealFactor(rel: RelNode): RelNode = {
    rel match {
      case hep: HepRelVertex => hep.getCurrentRel
      case _ => rel
    }
  }

  protected def isEqualsFilterWithInputRef(filter: RexNode, ref: Int): Boolean = {
    filter match {
      case call: RexCall if call.getOperator == SqlStdOperatorTable.EQUALS =>
        val operand0 = call.getOperands.head
        val operand1 = call.getOperands.last
        (operand0, operand1) match {
          case (op0: RexInputRef, op1: RexInputRef) =>
            op0.getIndex == ref || op1.getIndex == ref
          case _ => false
        }
      case _ => false
    }
  }

  protected def getAggCollation(
    aggCallKind: SqlKind,
    newAggOffset: Int): RelFieldCollation = {
    aggCallKind match {
      case SqlKind.MAX =>
        new RelFieldCollation(newAggOffset, Direction.DESCENDING)
      case SqlKind.MIN =>
        new RelFieldCollation(newAggOffset, Direction.ASCENDING)
      case _ =>
        throw new RuntimeException(s"Unexpected agg kind: $aggCallKind.")
    }
  }

  /**
    * Delete a factor from the ori multijoin and rebuild it.
    *
    * <p>Caution that we do not reset:
    * <pre>
    * oriRel.getProjFields,
    * oriRel.getJoinFieldRefCountsMap,
    * oriRel.getPostJoinFilter</pre> Cause they are not needed now!
    *
    * @param oriRel         original multijoin [[MultiJoin]] instance.
    * @param ori            original multijoin [[LoptMultiJoin]] instance.
    * @param deleteFactorId the factor id to be deleted.
    * @return a rebuilt [[LoptMultiJoin]] instance.
    */
  protected def rebuildMultiJoin(
    dataTypeFactory: RelDataTypeFactory,
    rexBuilder: RexBuilder,
    oriRel: MultiJoin,
    ori: LoptMultiJoin,
    deleteFactorId: Int): LoptMultiJoin = {
    // Rebuild the condition.
    val deleteStart = ori.getJoinStart(deleteFactorId)
    val numDeleteFields = ori.getJoinFactor(deleteFactorId).getRowType.getFieldCount
    val deleteEnd = deleteStart + numDeleteFields - 1
    val newFilters = ori.getJoinFilters
      .filter(f => !refFactors(f, ori).get(deleteFactorId))
      .map(RexUtil.shift(_, deleteEnd, -numDeleteFields))
    // Fix the factor indices.
    var factorIds = ImmutableBitSet.range(ori.getNumJoinFactors)
    val newFactors = new JArrayList[RelNode]()
    factorIds = factorIds.clear(deleteFactorId)
    newFactors ++= factorIds.map(factorIdx => ori.getJoinFactor(factorIdx))
    val oriType = oriRel.getRowType
    val newFields = oriType.getFieldList
      .filter(field => field.getIndex < deleteStart || field.getIndex > deleteEnd)
    val newType = dataTypeFactory
      .createStructType(newFields.map(f => f.getType), newFields.map(f => f.getName))

    val outerJoinConditions = new JArrayList[RexNode](oriRel.getOuterJoinConditions)
    outerJoinConditions.remove(deleteFactorId)
    val joinTypes = new JArrayList[JoinRelType](oriRel.getJoinTypes)
    joinTypes.remove(deleteFactorId)

    val newRel = new MultiJoin(oriRel.getCluster,
      newFactors,
      RexUtil.composeConjunction(rexBuilder, newFilters, false),
      newType,
      oriRel.isFullOuterJoin,
      outerJoinConditions,
      joinTypes,
      oriRel.getProjFields,
      oriRel.getJoinFieldRefCountsMap,
      oriRel.getPostJoinFilter)
    new LoptMultiJoin(newRel)
  }

  /**
    * Construct the final plan, will append the rank op to the rel tree,
    * this requires the group correlate not empty.
    * <ol>
    * <li>Find the agg cor column in outer multi-join.</li>
    * <li>Find the group cor column.</li>
    * </ol>
    */
  protected def constructFinalPlan(
    relBuilder: RelBuilder,
    multiJoinRel: MultiJoin,
    multiJoin: LoptMultiJoin,
    aggFactorIdx: Int,
    mq: RelMetadataQuery,
    aggGroupCorFactorIndex: Int,
    groupOffset: Int,
    aggCorFactorIndex: Int,
    aggOffset: Int,
    aggCallKind: SqlKind,
    call: RelOptRuleCall): Unit = {
    val sequenceTracer = new SequenceTracer
    val rebuiltTree = rebuildMultiJoin(
      relBuilder.getTypeFactory,
      relBuilder.getRexBuilder,
      multiJoinRel,
      multiJoin,
      aggFactorIdx)
    val rel = RewriteSelfJoinRule.buildTree(relBuilder,
      mq,
      rebuiltTree,
      sequenceTracer)

    val newGroupOffset = newFieldOffset(
      rebuiltTree,
      sequenceTracer.getFactors,
      aggGroupCorFactorIndex,
      groupOffset)
    val newAggOffset = newFieldOffset(
      rebuiltTree,
      sequenceTracer.getFactors,
      aggCorFactorIndex,
      aggOffset)

    val fieldCollation = getAggCollation(aggCallKind, newAggOffset)

    val newInputRef = buildNewInputRefs(
      relBuilder.getRexBuilder,
      rel,
      ImmutableBitSet.of(newGroupOffset, newAggOffset),
      multiJoin.getJoinFactor(aggFactorIdx).getRowType)

    val rank = relBuilder
      .push(rel).asInstanceOf[FlinkRelBuilder]
      .rank(
        SqlStdOperatorTable.RANK,
        ImmutableBitSet.of(newGroupOffset),
        RelCollations.of(fieldCollation),
        ConstantRankRange(1, 1))
      // Are the agg always in the end ?
      .project(newInputRef)
      .build()
    call.transformTo(rank)
  }

  /**
    * A shuttle impl to find the equivalence node, only allows nested node
    * under project if it exists.
    *
    * @param ori the node to compare with.
    */
  protected class EquivalentNodeFinder(ori: RelNode) extends RelShuttleImpl {
    private var found: Option[RelNode] = Option.empty
    private var valid: Boolean = true

    override def visitChildren(rel: RelNode): RelNode = {
      rel match {
        case node: RelNode if found.isEmpty && eqNode(getRealFactor(node), ori) =>
          found = Some(getRealFactor(node))
        case hepVertex: HepRelVertex =>
          val curRel = hepVertex.getCurrentRel
          // We only accept project above the ori node.
          if (found.isEmpty && !curRel.isInstanceOf[LogicalProject]) {
            valid = false
          }
          curRel.accept(this)
        case _ =>
      }
      super.visitChildren(rel)
    }

    def getEquivalence: Option[RelNode] = found

    def isValid: Boolean = valid
  }

  /**
    * Finder to fetch the nested [[MultiJoin]]. This can only work for a [[LoptMultiJoin]]
    * rel node.
    */
  protected class MultiJoinFinder extends RelShuttleImpl {
    private var found: Option[MultiJoin] = Option.empty

    override def visitChildren(rel: RelNode): RelNode = {
      rel match {
        case hepVertex: HepRelVertex =>
          val curRel = hepVertex.getCurrentRel
          curRel match {
            case multiJoin: MultiJoin if found.isEmpty =>
              found = Some(multiJoin)
            case _ =>
          }
          curRel.accept(this)
        case _ =>
      }
      super.visitChildren(rel)
    }

    def getMultiJoin: Option[MultiJoin] = found
  }

  /** We can update the prj ref only when the underlying factors are simple. **/
  protected class CorrelateFactorFinder(
    var inputRef: RexInputRef) extends RelShuttleImpl {
    private var groupByOffsetInFactor: Int = -1
    private var correlateFactor: RelNode = _
    private var correlateFactorIdx: Int = -1

    // Expect logic plan like:
    // LogicalAggregate(group=[{0}], EXPR$0=[MIN($1)])
    // +- LogicalProject(a=[$1], b=[$0])
    //    +- LogicalProject(b=[$3], a=[$0])
    //       +- MultiJoin()
    override def visitChildren(rel: RelNode): RelNode = {
      rel match {
        case hepVertex: HepRelVertex =>
          val curRel = hepVertex.getCurrentRel
          curRel match {
            case multiJoinRel: MultiJoin =>
              findCorrelate(multiJoinRel)
            case prj: LogicalProject =>
              prj.getProjects.get(inputRef.getIndex) match {
                case ref: RexInputRef =>
                  inputRef = ref
                case _ =>
              }
            case _ =>
          }
          curRel.accept(this)
        case _ =>
      }
      super.visitChildren(rel)
    }

    private def findCorrelate(multiJoinRel: MultiJoin): Unit = {
      val multiJoin = new LoptMultiJoin(multiJoinRel)
      val fieldIdx = inputRef.getIndex
      for (factorIdx <- 0 until multiJoin.getNumJoinFactors) {
        val startOffset = multiJoin.getJoinStart(factorIdx)
        val endOffset = startOffset + multiJoin.getNumFieldsInJoinFactor(factorIdx) - 1
        if (startOffset <= fieldIdx && fieldIdx <= endOffset) {
          // find the factor!
          correlateFactor = multiJoin.getJoinFactor(factorIdx)
          correlateFactorIdx = factorIdx
          groupByOffsetInFactor = fieldIdx - startOffset
        }
      }
    }

    def getGroupByOffsetInFactor: Int = groupByOffsetInFactor

    def getCorrelateFactor: RelNode = correlateFactor

    def getCorrelateFactorIdx: Int = correlateFactorIdx

    def notFound: Boolean = correlateFactor == null || correlateFactorIdx == -1
  }

}

/** This rule matches a multi-join view with agg filter on itself. In order to simplify
  * the pattern, for plan1 we only match successfully if there conditions are all satisfied:
  * <ol>
  * <li>Simple factors that have only filter or project on table scan.</li>
  * <li>Agg can have at most 1 group and only 1 grouping field, also 1 agg call.</li>
  * <li>The outer multi-join can have filters on it only if the factor is correlated
  * on unique keys with the agg.</li>
  * <li>Only Max/Min aggregation is allowed.</li>
  * <li>The outer and inner multi-join factors should equals to each other
  * with semanteme, except for the correlated factor.</li>
  * </ol>
  *
  * <pre>
  * plan1:
  *   prj
  *   +- prj
  *      +- MultiJoin
  *         :- factor1
  *         :- factor2
  *         :- factor3
  *         :- factor4
  *         :- factor5
  *         +- agg
  *            +- prj
  *               +- prj
  *                  +- MultiJoin
  *                     :- factor2
  *                     :- factor3
  *                     :- factor4
  *                     :- factor5
  * </pre>
  *
  * After the promotion, the plan1 would change to:
  * <pre>
  *   prj
  *   +- prj
  *      +- rank
  *         +- join
  *            :- join
  *            :  :- factor1
  *            :  +- join
  *            :     :- factor2
  *            :     +- factor3
  *            +- join
  *               :- factor4
  *               +- factor5
  * </pre>
  */
class RewriteMultiJoinRule extends RewriteSelfJoinRule {

  /**
    * If the factor is matched with the specified condition.
    *
    * @param factIdx           Factor index to decide from.
    * @param multiJoin         [[LoptMultiJoin]] instance.
    * @param mq                [[RelMetadataQuery]] instance.
    */
  private def matchedFactor(
    factIdx: Int,
    multiJoin: LoptMultiJoin,
    mq: RelMetadataQuery): Boolean = {
    // Only cares about INNER join.
    // Do not allow self join.
    if (isNonInnerJoinOrSelfJoin(multiJoin, factIdx)) {
      return false
    }
    val rel = multiJoin.getJoinFactor(factIdx)
    // If can only have a MAX/MIN agg.
    val maxMinAggMatched = isMaxMinAggMatched(rel)

    // If the factor is not simple, we expect it to be an agg.
    val simpleMatched = isSimpleFactorMatched(rel, mq)

    // If expect a multi-join under the agg.
    val multiJoinMatched = isMultiJoinMatched(rel)

    maxMinAggMatched && simpleMatched && multiJoinMatched
  }


  override def matches(call: RelOptRuleCall): Boolean = {
    val multiJoinRel: MultiJoin = call.rel(0)
    val multiJoin = new LoptMultiJoin(multiJoinRel)
    val mq = call.getMetadataQuery

    if (multiJoin.getMultiJoinRel.isFullOuterJoin) {
      return false
    }
    // Exists only one agg in the factors.
    val factorIdxes = 0 until multiJoin.getNumJoinFactors
    val factors = factorIdxes.map(factorIdx => multiJoin.getJoinFactor(factorIdx))

    factors.count(getRealFactor(_).isInstanceOf[LogicalAggregate]) == 1 &&
      factorIdxes.forall(matchedFactor(_, multiJoin, mq))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val multiJoinRel: MultiJoin = call.rel(0)
    val multiJoin = new LoptMultiJoin(multiJoinRel)
    val mq = call.getMetadataQuery
    val relBuilder = call.builder()

    // 1. First we need to make sure all the join nodes are simple join plus filter,
    // the outer multijoin should have only one nested multijoin which has a underlying
    // agg.

    // 2. Pick out the filters in outer multi-join and inner multi-join,
    // they should be either on both sides or in the same opt table which contains the
    // unique correlate column.
    val nJoinFactors = multiJoin.getNumJoinFactors
    var aggFactorIdx = -1

    for (factorIdx <- 0 until nJoinFactors) {
      val currentRel = getRealFactor(multiJoin.getJoinFactor(factorIdx))
      // Pick out agg node.
      if (currentRel.isInstanceOf[LogicalAggregate]) {
        aggFactorIdx = factorIdx
      }
    }
    require(aggFactorIdx != -1)
    val agg = getRealFactor(multiJoin.getJoinFactor(aggFactorIdx))
      .asInstanceOf[LogicalAggregate]
    // Skip multi groupings, do not support it now.
    // TODO: support multiple grouping fields and empty grouping set.
    if (agg.getGroupCount != 1
      || agg.getGroupSets.size > 1
      || agg.getAggCallList.size != 1) {
      return
    }
    val aggCallKind = agg.getAggCallList.head.getAggregation.getKind
    // Find the group factor in agg side.
    val groupInputRef = RexInputRef.of(agg.getGroupSet.head, agg.getRowType)
    val groupFactorFinder = new CorrelateFactorFinder(groupInputRef)
    groupFactorFinder.visit(agg)
    if (groupFactorFinder.notFound) {
      return
    }
    // Find the group factor in agg cor side.
    // Assume that the agg column is on the right hand side of output,
    // and group fields num = 1.
    val groupFieldsStart = multiJoin.getJoinStart(aggFactorIdx)
    // TODO: support multiple grouping filters.
    val groupFilter = getFactorSingleFilter(multiJoin, groupFieldsStart).getOrElse(return)
    val aggGroupCorFactorIndex = multiJoin.getFactorsRefByJoinFilter(groupFilter)
      .clear(aggFactorIdx)
      .head
    val groupOffset = relationFieldOffset(
      multiJoin,
      groupFilter,
      aggGroupCorFactorIndex,
      groupFieldsStart)
    val aggFieldsStart = groupFieldsStart + 1
    val aggFilter = getFactorSingleFilter(multiJoin, aggFieldsStart).getOrElse(return)
    val aggCorFactorIndex = multiJoin.getFactorsRefByJoinFilter(aggFilter)
      .clear(aggFactorIdx)
      .head
    val aggOffset = relationFieldOffset(
      multiJoin,
      aggFilter,
      aggCorFactorIndex,
      aggFieldsStart)

    // Group can only have extra filter when the cor column is unique
    val group = getRealFactor(multiJoin.getJoinFactor(aggGroupCorFactorIndex))
    if (group.isInstanceOf[LogicalFilter]) {
      val uniqueKeys = mq.getUniqueKeys(group.getInput(0))
      if (uniqueKeys == null
        || !uniqueKeys.exists(keys => keys.cardinality == 1 && keys.get(groupOffset))) {
        return
      }
    }

    // Find the group factors on both sides, now can begin our work!

    // Validate factors equality, exclude agg factor and group cor
    // factor from outer multi join.
    val multiJoinFinder = new MultiJoinFinder
    agg.accept(multiJoinFinder)
    val aggMultiJoinRel = multiJoinFinder.getMultiJoin.get
    require(aggMultiJoinRel != null)
    val aggMultiJoin = new LoptMultiJoin(aggMultiJoinRel)
    // First validate that the cor group factor has filter in outer multi-join
    // same as the correlation filter.
    val corRelationEq = multiJoin.getJoinFilters.exists {
      f =>
        val ff = refFactors(f, multiJoin)
        ff.get(aggGroupCorFactorIndex) &&
          ff.cardinality == 2 &&
          eqNode(multiJoin.getJoinFactor(ff.clear(aggGroupCorFactorIndex).head),
            groupFactorFinder.getCorrelateFactor)
    }
    if (!corRelationEq) {
      return
    }
    // Then make sure the cor group factor do not have other filters except with the
    // agg correlation.
    val groupCorRelationCnt = multiJoin.getJoinFilters.count {
      f =>
        val factors = refFactors(f, multiJoin)
        factors.get(aggGroupCorFactorIndex) && !factors.get(aggFactorIdx)
    }
    if (groupCorRelationCnt > 1) {
      return
    }

    // Validate filters equality, remove the cor table and filters
    // in the outer multi-join.
    val filtersToCompare = multiJoin.getJoinFilters.filter {
      f =>
        val rf = refFactors(f, multiJoin)
        !rf.get(aggGroupCorFactorIndex) && !rf.get(aggFactorIdx)
    }
    if (!multiJoinEq(multiJoin, aggMultiJoin, filtersToCompare.toList)) {
      return
    }

    // 3. Push down these filters and tweak and shift the ref positions.
    // 4. Add in the rank operator and tag it with the sort attr.
    constructFinalPlan(relBuilder, multiJoinRel, multiJoin, aggFactorIdx, mq,
      aggGroupCorFactorIndex, groupOffset, aggCorFactorIndex, aggOffset, aggCallKind, call)
  }
}

/**
  * This rule matches a simple view with agg filter on itself. For `simple` we means
  * there is no nested MultiJoin in it.
  *
  * <p>For plan2 we only match successfully if there conditions are all satisfied:
  * <ol>
  * <li>Factor4 should be equal to one of factor{1 .. n} with semanteme.</li>
  * <li>Agg can have at most 1 group and only 1 grouping field, also 1 agg call.</li>
  * <li>TODO: The outer multi-join can have filters on it only if the factor is correlated
  * on unique keys with the agg.</li>
  * <li>Only Max/Min aggregation is allowed.</li>
  * </ol>
  * <pre>
  * plan2:
  *   MultiJoin
  *   +- factor1
  *   +- factor2
  *   +- factor3
  *      +- agg
  *         +- factor4
  * </pre>
  *
  * After the promotion, the plan1 would change to:
  * <pre>
  *   prj
  *   +- prj
  *      +- rank
  *         +- join
  *            :- join
  *            :  :- factor1
  *            :  +- join
  *            :     :- factor2
  *            :     +- factor3
  *            +- factor4
  * </pre>
  */
class RewriteSimpleJoinRule extends RewriteSelfJoinRule {
  override def matches(call: RelOptRuleCall): Boolean = {
    val multiJoinRel: MultiJoin = call.rel(0)
    val multiJoin = new LoptMultiJoin(multiJoinRel)

    if (multiJoin.getMultiJoinRel.isFullOuterJoin) {
      return false
    }
    // Exists only one agg in the factors.
    val factors = mutable.ArrayBuffer[RelNode]()
    val factorIdxes = 0 until multiJoin.getNumJoinFactors
    for (factorIdx <- factorIdxes) {
      factors += multiJoin.getJoinFactor(factorIdx)
    }

    factorIdxes.forall(!isNonInnerJoinOrSelfJoin(multiJoin, _))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val multiJoinRel: MultiJoin = call.rel(0)
    val multiJoin = new LoptMultiJoin(multiJoinRel)
    val mq = call.getMetadataQuery
    val relBuilder = call.builder()

    // For simple mode, there do not have any nested multi-join,
    // we will use digest directly to match all the factors.

    val nJoinFactors = multiJoin.getNumJoinFactors
    var aggFactorIdx = -1
    var equivalenceUnderAgg: RelNode = null
    var aggFactorRowType: RelDataType = null

    // Firstly, find the agg above equivalence node, for a rel tree blow:
    // MultiJoin
    // +- factor1
    // +- factor2
    // +- factor3
    //    +- agg
    //       +- equi-node
    // we will loop from the factor {1 -> n} to search for the agg node under what there exists a
    // equivalence node of this factor, for equivalence we means that the digests are equal.
    (0 until nJoinFactors).foreach {
      factorIdx =>
        val currentRel = getRealFactor(multiJoin.getJoinFactor(factorIdx))
        (0 until nJoinFactors).filter(_ != factorIdx).foreach {
          targetIdx =>
            val finder = new EquivalentNodeFinder(currentRel)
            val targetRel = getRealFactor(multiJoin.getJoinFactor(targetIdx))
            targetRel.accept(finder)
            // Found an equivalence under agg.
            if (finder.getEquivalence.nonEmpty
              && finder.isValid
              && targetRel.isInstanceOf[LogicalAggregate]) {
              equivalenceUnderAgg = finder.getEquivalence.get
              aggFactorIdx = targetIdx
              aggFactorRowType = targetRel.getRowType
            }
        }
    }
    if (equivalenceUnderAgg == null) {
      return
    }
    require(aggFactorIdx != -1)
    val agg = getRealFactor(multiJoin.getJoinFactor(aggFactorIdx))
      .asInstanceOf[LogicalAggregate]
    // Skip multi groupings, do not support it now.
    // TODO: support multiple grouping fields.
    if (agg.getGroupCount > 1
      || agg.getGroupSets.size > 1
      || agg.getAggCallList.size != 1) {
      return
    }
    val aggCallKind = agg.getAggCallList.head.getAggregation.getKind
    // The agg above equivalence node must be only Max/Min.
    if (aggCallKind != SqlKind.MAX && aggCallKind != SqlKind.MIN) {
      return
    }

    // Find the group factor in agg side.
    var aggGroupCorFactorIndex = -1
    var groupOffset = -1
    var aggCorFactorIndex = -1
    var aggOffset = -1
    // If the group set is empty, it means the whole input is the group, so we do not need to
    // find and tweak the group cor column input ref.
    if (agg.getGroupSet.isEmpty) {
      // The whole input as group.
      val aggFieldsStart = multiJoin.getJoinStart(aggFactorIdx)
      val aggFilter = getFactorSingleFilter(multiJoin, aggFieldsStart).getOrElse(return)

      aggCorFactorIndex = multiJoin.getFactorsRefByJoinFilter(aggFilter)
        .clear(aggFactorIdx)
        .head
      aggOffset = relationFieldOffset(
        multiJoin,
        aggFilter,
        aggCorFactorIndex,
        aggFieldsStart)
      // No group set, we push down the rank to the cor factor, which is same as
      // aggCorFactorIndex here.
      val rebuiltTree = rebuildMultiJoin(
        relBuilder.getTypeFactory,
        relBuilder.getRexBuilder,
        multiJoinRel,
        multiJoin,
        aggFactorIdx)

      val rankInfo = RankInfo(aggCorFactorIndex, aggOffset, getAggCollation(aggCallKind, aggOffset))

      val sequenceTracer = new SequenceTracer
      val rel = RewriteSelfJoinRule.buildTree(relBuilder,
        mq,
        rebuiltTree,
        sequenceTracer,
        rankInfo = rankInfo)
      val newAggOffset = newFieldOffset(
        rebuiltTree,
        sequenceTracer.getFactors,
        aggCorFactorIndex,
        aggOffset)
      val newInputRef = buildNewInputRefs(
        relBuilder.getRexBuilder,
        rel,
        ImmutableBitSet.of(newAggOffset),
        aggFactorRowType)
      val finalTree = relBuilder
        .push(rel)
        .project(newInputRef)
        .build()
      call.transformTo(finalTree)
    } else {
      // Find the group factor in agg cor side.
      // Assume that the agg column is on the right hand side of output,
      // and group fields num = 1.
      val groupFieldsStart = multiJoin.getJoinStart(aggFactorIdx)
      // TODO: support multiple grouping filters.
      val groupFilter = getFactorSingleFilter(multiJoin, groupFieldsStart).getOrElse(return)

      require(groupFilter != null)
      aggGroupCorFactorIndex = multiJoin.getFactorsRefByJoinFilter(groupFilter)
        .clear(aggFactorIdx)
        .head
      groupOffset = relationFieldOffset(
        multiJoin,
        groupFilter,
        aggGroupCorFactorIndex,
        groupFieldsStart)

      // Then make sure the cor group factor do not have other filters except with the
      // agg correlation.
      val groupCorRelationCnt = multiJoin
        .getJoinFilters.count(f => {
        val factors = refFactors(f, multiJoin)
        factors.get(aggGroupCorFactorIndex) && !factors.get(aggFactorIdx)
      })
      if (groupCorRelationCnt > 1) {
        return
      }

      val aggFieldsStart = groupFieldsStart + 1
      val aggFilter = getFactorSingleFilter(multiJoin, aggFieldsStart).getOrElse(return)
      aggCorFactorIndex = multiJoin.getFactorsRefByJoinFilter(aggFilter)
        .clear(aggFactorIdx)
        .head
      aggOffset = relationFieldOffset(
        multiJoin,
        aggFilter,
        aggCorFactorIndex,
        aggFieldsStart)
      constructFinalPlan(relBuilder, multiJoinRel, multiJoin, aggFactorIdx, mq,
        aggGroupCorFactorIndex, groupOffset, aggCorFactorIndex, aggOffset, aggCallKind, call)
    }
  }
}

object RewriteSelfJoinRule {
  val COMPLEX = new RewriteMultiJoinRule
  val SIMPLE = new RewriteSimpleJoinRule


  /**
    * Build a join tree from a [[LoptMultiJoin]], will try to build the join tree in order as it
    * originally is, see [[buildTree]] #chooseNextEdge for details.
    *
    * <p>We also support push down the Rank operator.
    *
    * @param multiJoin      [[LoptMultiJoin]] instance.
    * @param sequenceTracer tracer to record the final join sequence.
    * @return join rel node.
    */
  def buildTree(
    relBuilder: RelBuilder,
    mq: RelMetadataQuery,
    multiJoin: LoptMultiJoin,
    sequenceTracer: SequenceTracer = null,
    rankInfo: RankInfo = null): RelNode = {
    val vertexes = mutable.ArrayBuffer[Vertex]()
    var fieldOffset = 0
    val oriFactors = ImmutableBitSet.range(multiJoin.getNumJoinFactors)
    oriFactors.foreach {
      factorIdx =>
        var factorRel = multiJoin.getJoinFactor(factorIdx)
        if (rankInfo != null && rankInfo.factorIdx == factorIdx) {
          relBuilder.push(factorRel)
          val projects = relBuilder.fields()
          factorRel = relBuilder.asInstanceOf[FlinkRelBuilder]
            .rank(
              SqlStdOperatorTable.RANK,
              ImmutableBitSet.of(),
              RelCollations.of(rankInfo.fieldCollation),
              ConstantRankRange(1, 1))
            .project(projects)
            .build()
        }
        vertexes += new LeafVertex(factorIdx, factorRel, fieldOffset)
        fieldOffset += factorRel.getRowType.getFieldCount
    }

    val unusedEdges = new JArrayList[Edge]()
    unusedEdges ++= multiJoin.getJoinFilters.map(f => createEdge(f, multiJoin))

    val usedEdges = new ArrayBuffer[Edge]()

    def allVertexesJoined(): Boolean = {
      // Always choose the next edge.
      if (unusedEdges.isEmpty) {
        // No more edges. Are there any un-joined vertexes?
        val lastVertex = vertexes.last
        val z = lastVertex.factors.previousClearBit(lastVertex.id - 1)
        if (z < 0) {
          return true
        }
      }
      false
    }

    def chooseNextEdge(unused: JArrayList[Edge]): Edge = {
      for (factorIdx <- oriFactors) {
        val nextCandidate = unused.filter(edge => edge.factors.get(factorIdx))
        if (nextCandidate.nonEmpty) {
          nextCandidate.sortWith((e1, e2) =>
            e1.factors.clear(factorIdx).head <= e2.factors.clear(factorIdx).head).head
        }
      }
      unused.head
    }

    while (!allVertexesJoined) {
      val factors: Array[Int] = unusedEdges match {
        case unused: JList[Edge] if unused.isEmpty =>
          // Choose left un-joined vertexes.
          val lastVertex = vertexes.last
          val z = lastVertex.factors.previousClearBit(lastVertex.id - 1)
          Array(z, lastVertex.id)
        case _ =>
          // Always choose the first edge(relation).
          val bestEdge = chooseNextEdge(unusedEdges)
          // Assume that the edge is between precisely two factors.
          require(bestEdge.factors.cardinality == 2)
          bestEdge.factors.toArray
      }

      val majorFactor = factors.head
      val minorFactor = factors.last
      val majorVertex = vertexes(majorFactor)
      val minorVertex = vertexes(minorFactor)

      // Find the join conditions. All conditions whose factors are now all in
      // the join can now be used.
      val v = vertexes.size
      val newFactors = majorVertex.factors
        .rebuild()
        .addAll(minorVertex.factors)
        .set(v)
        .build()
      val conditions = new ArrayBuffer[RexNode]()
      val itr = unusedEdges.iterator()
      while (itr.hasNext) {
        val nextEdge = itr.next()
        if (newFactors.contains(nextEdge.factors)) {
          conditions += nextEdge.condition
          itr.remove()
          usedEdges += nextEdge
        }
      }

      val newVertex =
        new JoinVertex(v, majorFactor, minorFactor, newFactors,
          ImmutableList.copyOf(conditions.toArray))
      vertexes += newVertex

      val merged = ImmutableBitSet.of(minorFactor, majorFactor)
      unusedEdges.zipWithIndex.foreach {
        case (edge, idx) =>
          if (edge.factors.intersects(merged)) {
            val newEdgeFactors = edge.factors
              .rebuild()
              .removeAll(newFactors)
              .set(v)
              .build()
            require(newEdgeFactors.cardinality == 2)
            val newEdge = new Edge(edge.condition, newEdgeFactors, edge.columns)
            unusedEdges.set(idx, newEdge)
          }
      }
    }

    // We have a winner!
    val relNodes = new ArrayBuffer[Pair[RelNode, Mappings.TargetMapping]]()
    vertexes.foreach {
      case leafVertex: LeafVertex =>
        val mapping = Mappings.offsetSource(
          Mappings.createIdentity(leafVertex.rel.getRowType.getFieldCount),
          leafVertex.fieldOffset,
          multiJoin.getNumTotalFields)
        relNodes += Pair.of(leafVertex.rel, mapping)
      case joinVertex: JoinVertex =>
        val leftPair = relNodes(joinVertex.leftFactor)
        val left = leftPair.left
        val leftMapping = leftPair.right
        val rightPair = relNodes(joinVertex.rightFactor)
        val right = rightPair.left
        val rightMapping = rightPair.right
        val mapping = Mappings.merge(leftMapping,
          Mappings.offsetTarget(rightMapping, left.getRowType.getFieldCount))
        val shuttle = new RexPermuteInputsShuttle(mapping, left, right)
        val condition = RexUtil.composeConjunction(relBuilder.getRexBuilder,
          joinVertex.conditions, false)
        val join = relBuilder.push(left)
          .push(right)
          .join(JoinRelType.INNER, condition.accept(shuttle))
          .build()
        relNodes += Pair.of(join, mapping)
      case _ =>
    }
    if (sequenceTracer != null) {
      sequenceTracer.setFactors(vertexes.last.factors)
    }
    val top = relNodes.last
    relBuilder.push(top.left)
      .project(relBuilder.fields(top.right))
    relBuilder.build()
  }

  private def createEdge(condition: RexNode, multiJoin: LoptMultiJoin): Edge = {
    val fieldRefBitmap = RelOptUtil.InputFinder.bits(condition)
    val factorRefBitmap = ImmutableBitSet.builder
    for (field <- fieldRefBitmap) {
      val factor = multiJoin.findRef(field)
      factorRefBitmap.set(factor)
    }
    new Edge(condition, factorRefBitmap.build(), fieldRefBitmap)
  }

  /** Participant in a join (relation or join). */
  private class Vertex(
    val id: Int,
    val factors: ImmutableBitSet) {
  }

  /** Relation participating in a join. */
  private class LeafVertex(
    override val id: Int,
    val rel: RelNode,
    val fieldOffset: Int) extends Vertex(id, ImmutableBitSet.of(id)) {

    override def toString: String = {
      s"LeafVertex(id: $id, " +
        s"factors: $factors, " +
        s"fieldOffset: $fieldOffset)"
    }
  }

  /** Participant in a join which is itself a join. */
  private class JoinVertex(
    override val id: Int,
    val leftFactor: Int,
    val rightFactor: Int,
    override val factors: ImmutableBitSet,
    val conditions: ImmutableList[RexNode]) extends Vertex(id, factors) {

    override def toString: String = {
      s"JoinVertex(id: $id, " +
        s"factors: $factors, " +
        s"leftFactor: $leftFactor, " +
        s"rightFactor: $rightFactor)"
    }
  }

  /** Information about a join-condition. */
  private class Edge(
    val condition: RexNode,
    val factors: ImmutableBitSet,
    val columns: ImmutableBitSet) {

    override def toString: String = {
      s"Edge(condition: $condition, " +
        s"factors: $factors, " +
        s"columns: $columns)"
    }
  }

  /** Used for Rank push down. */
  case class RankInfo(factorIdx: Int, aggOffset: Int, fieldCollation: RelFieldCollation)

  /** Used for trace the new factors sequence. */
  class SequenceTracer {
    var factors: ImmutableBitSet = _

    def getFactors: ImmutableBitSet = {
      factors
    }

    def setFactors(factors: ImmutableBitSet): Unit = {
      this.factors = factors
    }
  }
}
