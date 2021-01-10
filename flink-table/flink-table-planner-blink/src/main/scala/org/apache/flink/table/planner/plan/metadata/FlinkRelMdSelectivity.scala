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

import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.utils.FlinkRelMdUtil
import org.apache.flink.table.planner.{JArrayList, JDouble}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.{RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{BuiltInMethod, ImmutableBitSet, Util}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdSelectivity supplies a implementation of
  * [[RelMetadataQuery#getSelectivity]] for the standard logical algebra.
  */
class FlinkRelMdSelectivity private extends MetadataHandler[BuiltInMetadata.Selectivity] {

  def getDef: MetadataDef[BuiltInMetadata.Selectivity] = BuiltInMetadata.Selectivity.DEF

  def getSelectivity(rel: TableScan, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    estimateSelectivity(rel, mq, predicate)

  def getSelectivity(rel: Project, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    estimateSelectivity(rel, mq, predicate)

  def getSelectivity(rel: Filter, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    estimateSelectivity(rel, mq, predicate)

  def getSelectivity(rel: Calc, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    estimateSelectivity(rel, mq, predicate)

  def getSelectivity(rel: Expand, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      1.0
    } else if (RelOptUtil.InputFinder.bits(predicate).toList.contains(rel.expandIdIndex)) {
      // sql like:
      // select count(*) as c, from emp group by rollup(deptno, gender) +
      // having grouping(deptno) <= grouping_id(deptno, gender)"
      // will trigger this case.
      RelMdUtil.guessSelectivity(predicate)
    } else {
      mq.getSelectivity(rel.getInput, predicate)
    }
  }

  def getSelectivity(rel: Exchange, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    mq.getSelectivity(rel.getInput, predicate)

  def getSelectivity(rel: Rank, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      return 1D
    }
    val (nonRankPred, rankPred) = FlinkRelMdUtil.splitPredicateOnRank(rel, predicate)
    val childSelectivity: JDouble = nonRankPred match {
      case Some(p) => mq.getSelectivity(rel.getInput, p)
      case _ => 1D
    }

    val rankSelectivity: JDouble = rankPred match {
      case Some(p) => estimateSelectivity(rel, mq, p)
      case _ => 1D
    }
    childSelectivity * rankSelectivity
  }

  def getSelectivity(rel: Sort, mq: RelMetadataQuery, predicate: RexNode): JDouble =
    mq.getSelectivity(rel.getInput, predicate)

  def getSelectivity(
      rel: Aggregate,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = getSelectivityOfAgg(rel, mq, predicate)

  def getSelectivity(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = getSelectivityOfAgg(rel, mq, predicate)

  def getSelectivity(
      rel: WindowAggregate,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = {
    val newPredicate = FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(rel, predicate)
    getSelectivityOfAgg(rel, mq, newPredicate)
  }

  def getSelectivity(
      rel: BatchPhysicalWindowAggregateBase,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = {
    val newPredicate = if (rel.isFinal) {
      FlinkRelMdUtil.makeNamePropertiesSelectivityRexNode(rel, predicate)
    } else {
      predicate
    }
    getSelectivityOfAgg(rel, mq, newPredicate)
  }

  private def getSelectivityOfAgg(
      agg: SingleRel,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      1.0
    } else {
      val hasLocalAgg = agg match {
        case _: Aggregate => false
        case rel: BatchPhysicalGroupAggregateBase => rel.isFinal && rel.isMerge
        case rel: BatchPhysicalWindowAggregateBase => rel.isFinal && rel.isMerge
        case _ => throw new IllegalArgumentException(s"Cannot handle ${agg.getRelTypeName}!")
      }
      if (hasLocalAgg) {
        val childPredicate = agg match {
          case rel: BatchPhysicalWindowAggregateBase =>
            // set the predicate as they correspond to local window aggregate
            FlinkRelMdUtil.setChildPredicateOfWinAgg(predicate, rel)
          case _ => predicate
        }
        return mq.getSelectivity(agg.getInput, childPredicate)
      }

      val (childPred, restPred) = agg match {
        case rel: Aggregate =>
          FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
        case rel: BatchPhysicalGroupAggregateBase =>
          FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
        case rel: BatchPhysicalWindowAggregateBase =>
          FlinkRelMdUtil.splitPredicateOnAggregate(rel, predicate)
        case _ => throw new IllegalArgumentException(s"Cannot handle ${agg.getRelTypeName}!")
      }
      val childSelectivity = mq.getSelectivity(agg.getInput(), childPred.orNull)
      if (childSelectivity == null) {
        null
      } else {
        val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
        val aggCallEstimator = new AggCallSelectivityEstimator(agg, fmq)
        val restSelectivity = aggCallEstimator.evaluate(restPred.orNull) match {
          case Some(s) => s
          case _ => RelMdUtil.guessSelectivity(restPred.orNull)
        }
        childSelectivity * restSelectivity
      }
    }
  }

  def getSelectivity(
      overWindow: Window,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = getSelectivityOfOverAgg(overWindow, mq, predicate)

  def getSelectivity(
      rel: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = getSelectivityOfOverAgg(rel, mq, predicate)

  private def getSelectivityOfOverAgg(
      over: SingleRel,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      1.0
    } else {
      val input = over.getInput
      val childBitmap = over match {
        case _: BatchExecOverAggregate | _: Window =>
          ImmutableBitSet.range(0, input.getRowType.getFieldCount)
        case _ => throw new IllegalArgumentException(s"Unknown node type ${over.getRelTypeName}")
      }
      val notPushable = new JArrayList[RexNode]
      val pushable = new JArrayList[RexNode]
      RelOptUtil.splitFilters(
        childBitmap,
        predicate,
        pushable,
        notPushable)
      val rexBuilder = over.getCluster.getRexBuilder
      val childPreds = RexUtil.composeConjunction(rexBuilder, pushable, true)
      val partSelectivity = mq.getSelectivity(input, childPreds)
      if (partSelectivity == null) {
        null
      } else {
        val rest = RexUtil.composeConjunction(rexBuilder, notPushable, true)
        val restSelectivity = RelMdUtil.guessSelectivity(rest)
        partSelectivity * restSelectivity
      }
    }
  }

  def getSelectivity(rel: Join, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue) {
      1.0
    } else {
      rel.getJoinType match {
        case JoinRelType.SEMI | JoinRelType.ANTI =>
          // create a RexNode representing the selectivity of the
          // semi-join filter and pass it to getSelectivity
          val rexBuilder = rel.getCluster.getRexBuilder
          var newPred = FlinkRelMdUtil.makeSemiAntiJoinSelectivityRexNode(mq, rel)
          if (predicate != null) {
            newPred = rexBuilder.makeCall(SqlStdOperatorTable.AND, newPred, predicate)
          }
          mq.getSelectivity(rel.getLeft, newPred)
        case _ =>
          estimateSelectivity(rel, mq, predicate)
      }
    }
  }

  def getSelectivity(rel: Union, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    if (predicate == null || predicate.isAlwaysTrue || rel.getInputs.size == 0) {
      1.0
    } else {
      // convert the predicate to reference the types of the union child
      val rexBuilder = rel.getCluster.getRexBuilder
      val adjustments = new Array[Int](rel.getRowType.getFieldCount)
      var inputRows: Seq[JDouble] = Nil
      var inputSelectedRows: Seq[JDouble] = Nil
      rel.getInputs foreach { input =>
        val inputRowCount = mq.getRowCount(input)
        inputRows = inputRows :+ inputRowCount
        val inputSelectedRow: JDouble = if (inputRowCount == null) {
          null
        } else {
          val modifiedPred = predicate.accept(
            new RelOptUtil.RexInputConverter(
              rexBuilder, null, input.getRowType.getFieldList, adjustments))
          val selectivity = mq.getSelectivity(input, modifiedPred)
          if (selectivity == null) null else selectivity * inputRowCount
        }
        inputSelectedRows = inputSelectedRows :+ inputSelectedRow
      }
      if (inputRows.contains(null) || inputSelectedRows.contains(null)) {
        null
      } else {
        val sumRows = inputRows.reduce(_ + _)
        val sumSelectedRows = inputSelectedRows.reduce(_ + _)
        if (sumRows < 1.0) sumSelectedRows else sumSelectedRows / sumRows
      }
    }
  }

  def getSelectivity(subset: RelSubset, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getSelectivity(rel, predicate)
  }

  // TODO only effects BatchPhysicalRel instead of all RelNode now
  def getSelectivity(rel: RelNode, mq: RelMetadataQuery, predicate: RexNode): JDouble = {
    rel match {
      case _: BatchPhysicalRel => estimateSelectivity(rel, mq, predicate)
      case _ => RelMdUtil.guessSelectivity(predicate)
    }
  }

  private def estimateSelectivity(
      rel: RelNode,
      mq: RelMetadataQuery,
      predicate: RexNode): JDouble = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val estimator = new SelectivityEstimator(rel, fmq)
    estimator.evaluate(predicate) match {
      case Some(s) => s
      case _ => RelMdUtil.guessSelectivity(predicate)
    }
  }

}

object FlinkRelMdSelectivity {

  private val INSTANCE = new FlinkRelMdSelectivity

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.SELECTIVITY.method, INSTANCE)

}
