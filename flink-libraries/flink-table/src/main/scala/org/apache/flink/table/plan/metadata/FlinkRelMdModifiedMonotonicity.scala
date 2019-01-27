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

package org.apache.flink.table.plan.metadata

import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.sql.SqlIncrSumAggFunction
import org.apache.flink.table.functions.utils.ScalarSqlFunction
import org.apache.flink.table.plan.FlinkJoinRelType
import org.apache.flink.table.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.plan.metadata.FlinkMetadata.ModifiedMonotonicityMeta
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.nodes.physical.batch.BatchExecGroupAggregateBase
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.schema.{DataStreamTable, IntermediateRelNodeTable}
import org.apache.flink.table.plan.stats.{WithLower, WithUpper}

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{AbstractRelNode, RelCollation, RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexCall, RexCallBinding, RexInputRef, RexNode}
import org.apache.calcite.sql.{SqlKind, SqlOperatorBinding}
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlMinMaxAggFunction, SqlSumAggFunction, SqlSumEmptyIsZeroAggFunction}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlMonotonicity._
import org.apache.calcite.util.{ImmutableIntList, Util}

import java.lang.{Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInt, Long => JLong, Short => JShort, String => JString}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.util
import java.util.{List => JList}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdModifiedMonotonicity supplies a default implementation of
  * [[FlinkRelMetadataQuery.getRelModifiedMonotonicity]] for logical algebra.
  */
class FlinkRelMdModifiedMonotonicity private extends MetadataHandler[ModifiedMonotonicityMeta] {

  override def getDef: MetadataDef[ModifiedMonotonicityMeta] =
    FlinkMetadata.ModifiedMonotonicityMeta.DEF

  /**
    * Utility to create a RelModifiedMonotonicity which all fields is modified constant which
    * means all the field's value will not be modified.
    */
  def constants(rel: RelNode): RelModifiedMonotonicity = {
    new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(CONSTANT))
  }

  def notMonotonic(rel: RelNode): RelModifiedMonotonicity = {
    new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(NOT_MONOTONIC))
  }

  // ---------------------------  Abstract RelNode ------------------------------

  def getRelModifiedMonotonicity(rel: RelNode, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val rowSize = rel.getRowType.getFieldCount
    rel match {
      case _: StreamExecCorrelate | _: Correlate =>
        getBasicMono(rel.getInput(0), mq, rowSize)
      case wa: StreamExecWatermarkAssigner =>
        getBasicMono(wa.getInput, mq, rowSize)
      case mb: StreamExecMiniBatchAssigner =>
        getBasicMono(mb.getInput, mq, rowSize)
      case _: StreamExecTemporalTableFunctionJoin =>
        getBasicMono(rel.getInput(0), mq, rowSize)
      case _: StreamExecExpand =>
        getBasicMono(rel.getInput(0), mq, rowSize)
      case _ => null
    }
  }

  def getRelModifiedMonotonicity(subset: RelSubset, mq: RelMetadataQuery)
  : RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(Util.first(subset.getBest, subset.getOriginal))
  }

  def getRelModifiedMonotonicity(
    hepRelVertex: HepRelVertex, mq: RelMetadataQuery): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(hepRelVertex.getCurrentRel)
  }

  def getRelModifiedMonotonicity(rel: TableScan, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val monotonicity: RelModifiedMonotonicity = rel match {
      case _: FlinkLogicalNativeTableScan | _: StreamExecDataStreamScan =>
        val table = rel.getTable.unwrap(classOf[DataStreamTable[Any]])
        table.statistic.getRelModifiedMonotonicity
      case _: FlinkLogicalIntermediateTableScan | _: StreamExecIntermediateTableScan =>
        val table = rel.getTable.unwrap(classOf[IntermediateRelNodeTable])
        table.statistic.getRelModifiedMonotonicity
      case _ => null
    }

    if (monotonicity != null) {
      monotonicity
    } else {
      new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(CONSTANT))
    }
  }

  def getRelModifiedMonotonicity(rel: Aggregate, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getAggModifiedMono(rel.getInput, mq, rel.getAggCallList.toList, rel.getGroupSet.toArray)
  }

  def getRelModifiedMonotonicity(calc: Calc, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
    getProjectMonotonicity(projects, calc.getInput, mq)
  }

  def getRelModifiedMonotonicity(rel: Project, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getProjectMonotonicity(rel.getProjects, rel.getInput, mq)
  }

  def getRelModifiedMonotonicity(rel: Union, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getUnionMonotonicity(rel, mq)
  }

  // --------------------------- FlinkLogical RelNode ------------------------------

  def getRelModifiedMonotonicity(
      rel: FlinkLogicalWindowAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    null
  }

  def getRelModifiedMonotonicity(
    rel: FlinkLogicalLastRow,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    if (allAppend(mq, rel.getInput)) {
      val mono = new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(MONOTONIC))
      rel.getUniqueKeys.foreach(e => mono.fieldMonotonicities(e) = CONSTANT)
      mono
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
    rel: FlinkLogicalOverWindow,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    constants(rel)
  }

  def getRelModifiedMonotonicity(join: FlinkLogicalJoin, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    val joinInfo = join.analyzeCondition
    val joinType = FlinkJoinRelType.toFlinkJoinRelType(join.getJoinType)
    getJoinMonotonicity(
      joinInfo,
      joinType,
      join.getLeft,
      join.getRight,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      mq)
  }

  def getRelModifiedMonotonicity(rel: FlinkLogicalUnion, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getUnionMonotonicity(rel, mq)
  }

  def getRelModifiedMonotonicity(
    rel: FlinkLogicalSemiJoin,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    if (!rel.isAnti) {
      val joinInfo = JoinInfo.of(rel.getLeft, rel.getRight, rel.getCondition)
      getJoinMonotonicity(
        joinInfo,
        FlinkJoinRelType.SEMI,
        rel.getLeft,
        rel.getRight,
        joinInfo.leftKeys,
        joinInfo.rightKeys,
        mq)
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(rel: FlinkLogicalRank, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getRankMonotonicity(rel.getInput,
      rel,
      rel.partitionKey.toArray,
      rel.outputRankFunColumn,
      rel.sortCollation,
      mq)
  }

  // --------------------------- StreamExec RelNode ------------------------------

  def getRelModifiedMonotonicity(rel: StreamExecUnion, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getUnionMonotonicity(rel, mq)
  }

  def getRelModifiedMonotonicity(rel: StreamExecGroupAggregate, mq: RelMetadataQuery)
  : RelModifiedMonotonicity = {
    getAggModifiedMono(rel.getInput, mq, rel.aggCalls.toList, rel.getGroupings)
  }

  def getRelModifiedMonotonicity(rel: StreamExecLocalGroupAggregate, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getAggModifiedMono(rel.getInput, mq, rel.aggCalls.toList, rel.getGroupings)
  }

  def getRelModifiedMonotonicity(rel: StreamExecGlobalGroupAggregate, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    // global and local agg should have same update mono
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(rel.getInput)
  }

  def getRelModifiedMonotonicity(rel: StreamExecIncrementalGroupAggregate, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getAggModifiedMono(rel.getInput, mq, rel.finalAggCalls.toList, rel.groupKey)
  }

  def getRelModifiedMonotonicity(rel: StreamExecGroupWindowAggregate, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    if (allAppend(mq, rel.getInput) && !rel.producesUpdates) {
      constants(rel)
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
    rel: StreamExecOverAggregate,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    constants(rel)
  }

  def getRelModifiedMonotonicity(
    rel: StreamExecExchange,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    // for exchange, get correspond from input mono
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(rel.getInput)
  }

  def getRelModifiedMonotonicity(rel: StreamExecWindowJoin, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    // window join won't have update
    constants(rel)
  }

  def getRelModifiedMonotonicity(rel: BatchExecGroupAggregateBase, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    null
  }

  def getRelModifiedMonotonicity(join: Join, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val joinInfo = join.analyzeCondition
    val joinType = FlinkJoinRelType.toFlinkJoinRelType(join.getJoinType)
    joinInfo.leftKeys.toIntArray
    getJoinMonotonicity(
      joinInfo,
      joinType,
      join.getLeft,
      join.getRight,
      joinInfo.leftKeys,
      joinInfo.rightKeys,
      mq)
  }

  def getRelModifiedMonotonicity(join: StreamExecJoin, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getJoinMonotonicity(
      join.joinInfo,
      join.joinType,
      join.getLeft,
      join.getRight,
      join.joinInfo.leftKeys,
      join.joinInfo.rightKeys,
      mq)
  }

  def getRelModifiedMonotonicity(rel: StreamExecRank, mq: RelMetadataQuery):
  RelModifiedMonotonicity = {
    getRankMonotonicity(
      rel.getInput,
      rel,
      rel.partitionKey.toArray,
      rel.outputRankFunColumn,
      rel.sortCollation,
      mq)
  }

  def getRelModifiedMonotonicity(
    rel: StreamExecLastRow,
    mq: RelMetadataQuery): RelModifiedMonotonicity = {

    if (allAppend(mq, rel.getInput)) {
      val mono = new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(MONOTONIC))
      rel.getUniqueKeys.foreach(e => mono.fieldMonotonicities(e) = CONSTANT)
      mono
    } else {
      null
    }
  }

  /*********  Common Function **********/

  def getUnionMonotonicity(rel: AbstractRelNode, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (rel.getInputs.exists(p => containsDelete(fmq, p))) {
      null
    } else {
      val monos = rel.getInputs.map(fmq.getRelModifiedMonotonicity)
      val head = monos.head
      if (monos.forall(head.equals(_))) {
        head
      } else {
        notMonotonic(rel)
      }
    }
  }

  def getJoinMonotonicity(
      joinInfo: JoinInfo,
      joinRelType: FlinkJoinRelType,
      left: RelNode,
      right: RelNode,
      leftKeys: ImmutableIntList,
      rightKeys: ImmutableIntList,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)

    // if group set contains update return null
    val containDelete = containsDelete(fmq, left) || containsDelete(fmq, right)
    val containUpdate = containsUpdate(fmq, left) || containsUpdate(fmq, right)

    val keyPaireAllAppend =
      leftKeys.toIntArray.forall({
        val mono = fmq.getRelModifiedMonotonicity(left)
        mono != null && mono.fieldMonotonicities(_) == CONSTANT
      }) &&
        rightKeys.toIntArray.forall({
          val mono = fmq.getRelModifiedMonotonicity(right)
          mono != null && mono.fieldMonotonicities(_) == CONSTANT
        })

    if (!containDelete &&
      !joinRelType.equals(FlinkJoinRelType.ANTI) &&
      keyPaireAllAppend &&
      (containUpdate && joinInfo.isEqui || !containUpdate)) {

      // output rowtype of semi equals to the rowtype of left child
      if (joinRelType.equals(FlinkJoinRelType.SEMI)) {
        fmq.getRelModifiedMonotonicity(left)
      } else {
        val lmono = fmq.getRelModifiedMonotonicity(left).fieldMonotonicities
        val rmono = fmq.getRelModifiedMonotonicity(right).fieldMonotonicities
        new RelModifiedMonotonicity(lmono ++ rmono)
      }

    } else {
      null
    }
  }

  def getAggModifiedMono(
      input: RelNode,
      mq: RelMetadataQuery,
      aggCallList: List[AggregateCall],
      groupSet: Array[Int]): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val childUpdateMono = fmq.getRelModifiedMonotonicity(input)
    val groupCnt = groupSet.length
    // if group by a update field or group by a field mono is null, just return null
    if (groupSet.exists(
      e => {
        childUpdateMono == null || childUpdateMono.fieldMonotonicities(e) != CONSTANT
      })) {
      return null
    }

    val currentUpdateMono = new RelModifiedMonotonicity(
      Array.fill(groupCnt)(CONSTANT) ++ Array.fill(aggCallList.size)(NOT_MONOTONIC))

    // get orig monotonicity ignore child
    aggCallList.zipWithIndex.foreach(
      e => {
        currentUpdateMono.fieldMonotonicities(e._2 + groupCnt) =
          getAggMonotonicity(e._1, fmq, input)
      })

    // need to reCalc monotonicity if child contains update
    val mono = currentUpdateMono.fieldMonotonicities
    if (containsUpdate(fmq, input)) {
      aggCallList.zipWithIndex.foreach(
        e => {
          if (e._1.getArgList.size() > 1) {
            mono(e._2 + groupCnt) = NOT_MONOTONIC
          } else if (e._1.getArgList.size() == 1) {
            val childMono = childUpdateMono.fieldMonotonicities(e._1.getArgList.head)
            val currentMono = mono(e._2 + groupCnt)
            // count will Increasing even child is NOT_MONOTONIC
            if (childMono != currentMono &&
              !e._1.getAggregation.isInstanceOf[SqlCountAggFunction]) {
              mono(e._2 + groupCnt) = NOT_MONOTONIC
            }
          }
        })
    }
    currentUpdateMono
  }

  def getProjectMonotonicity(
      projects: JList[RexNode],
      input: RelNode,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    // contains delete
    if (containsDelete(mq, input)) {
      return null
    }

    // all append
    if (allAppend(fmq, input)) {
      return new RelModifiedMonotonicity(Array.fill(projects.size)(CONSTANT))
    }

    // contains update
    // init mono
    val groups = new util.LinkedList[Int]()
    val monos = Array.fill(projects.size())(NOT_MONOTONIC)
    val childMono = fmq.getRelModifiedMonotonicity(input).fieldMonotonicities
    // copy child mono
    projects.zipWithIndex.foreach(
      e => {
        def getInputFieldIndex(node: RexNode): Int = {
          node match {
            case ref: RexInputRef =>
              if (ref.getIndex >= childMono.length) {
                println("")
              }
              monos(e._2) = childMono(ref.getIndex)
              ref.getIndex
            case a: RexCall if a.getKind == SqlKind.AS || a.getKind == SqlKind.CAST =>
              getInputFieldIndex(a.getOperands.get(0))
            case c: RexCall if c.getOperands.size() == 1 =>
              c.getOperator match {
                case ssf: ScalarSqlFunction =>
                  val inputIndex = getInputFieldIndex(c.getOperands.get(0))
                  val inputCollations = mq.collations(input)
                  val binding = RexCallBinding.create(
                    input.getCluster.getTypeFactory, c, inputCollations)
                  val udfMono = getUdfMonotonicity(ssf, binding)

                  val inputMono = if (inputIndex > -1) {
                    childMono(inputIndex)
                  } else {
                    NOT_MONOTONIC
                  }
                  if (inputMono == udfMono) {
                    monos(e._2) = inputMono
                  } else {
                    monos(e._2) = NOT_MONOTONIC
                  }
                  inputIndex
                case _ => -1
              }
            case _ => -1
          }
        }
        getInputFieldIndex(e._1)
      })
    new RelModifiedMonotonicity(monos)
  }

  def getRankMonotonicity(
      input: RelNode,
      rank: RelNode,
      partitionKey: Array[Int],
      hasRowNumber: Boolean,
      sortCollation: RelCollation,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val childUpdateMono = fmq.getRelModifiedMonotonicity(input)

    // If child monotonicity is null, we should return early.
    if (childUpdateMono == null) {
      return null
    }

    // if partitionBy a update field or partitionBy a field whose mono is null, just return null
    if (partitionKey.exists(
      e => {
        childUpdateMono == null || childUpdateMono.fieldMonotonicities(e) != CONSTANT
      })) {
      return null
    }

    // init current mono
    val currentUpdateMono = notMonotonic(rank)
    // 1. partitionBy field is CONSTANT
    partitionKey.foreach(e => currentUpdateMono.fieldMonotonicities(e) = CONSTANT)
    // 2. row number filed is CONSTANT
    if(hasRowNumber) {
      currentUpdateMono.fieldMonotonicities(rank.getRowType.getFieldCount - 1) = CONSTANT
    }
    // 3. time attribute field is increasing
    (0 until rank.getRowType.getFieldCount).foreach(e => {
      if (FlinkTypeFactory.isTimeIndicatorType(rank.getRowType.getFieldList.get(e).getType)) {
        childUpdateMono.fieldMonotonicities(e) = INCREASING
      }
    })
    val fieldCollations = sortCollation.getFieldCollations
    if (fieldCollations.nonEmpty) {
      // 4. process the first collation field, we can only deduce the first collation field
      val firstCollation = fieldCollations.get(0)
      // Collation field index in child node will be same with Rank node,
      // see ProjectToLogicalProjectAndWindowRule for details.
      val childFieldMono = childUpdateMono.fieldMonotonicities(firstCollation.getFieldIndex)
      currentUpdateMono.fieldMonotonicities(firstCollation.getFieldIndex) =
        childFieldMono match {
          case SqlMonotonicity.INCREASING | SqlMonotonicity.CONSTANT
            if firstCollation.direction == RelFieldCollation.Direction.DESCENDING => INCREASING
          case SqlMonotonicity.DECREASING | SqlMonotonicity.CONSTANT
            if firstCollation.direction == RelFieldCollation.Direction.ASCENDING => DECREASING
          case _ => NOT_MONOTONIC
        }
    }

    currentUpdateMono
  }

  /**
    * These operator won't generate update itself
    */
  def getBasicMono(
      input: RelNode,
      mq: RelMetadataQuery,
      rowSize: Int): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (containsDelete(fmq, input)) {
      null
    } else if (allAppend(fmq, input)) {
      new RelModifiedMonotonicity(Array.fill(rowSize)(CONSTANT))
    } else {
      new RelModifiedMonotonicity(Array.fill(rowSize)(NOT_MONOTONIC))
    }
  }

  def containsDelete(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(node) == null
  }

  def containsUpdate(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    !containsDelete(fmq, node) &&
      fmq.getRelModifiedMonotonicity(node).fieldMonotonicities.exists(_ != CONSTANT)
  }

  def allAppend(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    !containsDelete(fmq, node) &&
      fmq.getRelModifiedMonotonicity(node).fieldMonotonicities.forall(_ == CONSTANT)
  }

  def getUdfMonotonicity(udf: ScalarSqlFunction, binding: SqlOperatorBinding): SqlMonotonicity = {
    // get monotonicity info from ScalarSqlFunction directly.
    udf.getMonotonicity(binding)
  }

  def getAggMonotonicity(
    aggCall: AggregateCall,
    mq: RelMetadataQuery,
    input: RelNode): SqlMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)

    aggCall.getAggregation match {
      case _: SqlCountAggFunction => INCREASING
      case mm: SqlMinMaxAggFunction =>
        mm.kind match {
          case SqlKind.MAX => INCREASING
          case SqlKind.MIN => DECREASING
          case _ => NOT_MONOTONIC
        }

      case _: SqlIncrSumAggFunction => INCREASING

      case _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction =>
        val valueInterval = fmq.getFilteredColumnInterval(
          input, aggCall.getArgList.head, aggCall.filterArg)
        if (valueInterval == null) {
          NOT_MONOTONIC
        } else {
          valueInterval match {
            case n1: WithLower =>
              val compare = isValueGreaterThanZero(n1.lower)
              if (compare >= 0) {
                INCREASING
              } else {
                NOT_MONOTONIC
              }
            case n2: WithUpper =>
              val compare = isValueGreaterThanZero(n2.upper)
              if (compare <= 0) {
                DECREASING
              } else {
                NOT_MONOTONIC
              }
            case _ =>
              // value range has no lower end
              NOT_MONOTONIC
          }
        }
      case _ => NOT_MONOTONIC
    }
  }

  private def isValueGreaterThanZero[T](value: Comparable[T]): Int = {
    value match {
      case i: JInt => i.compareTo(0)
      case l: JLong => l.compareTo(0L)
      case db: JDouble => db.compareTo(0d)
      case f: JFloat => f.compareTo(0f)
      case s: JShort => s.compareTo(0.toShort)
      case b: JByte => b.compareTo(0.toByte)
      case dec: JBigDecimal => dec.compareTo(JBigDecimal.ZERO)
      case _: Date | _: Time | _: Timestamp | _: JString =>
        //not interested here, just return negative
        -1
      case _ =>
        // other numeric types
        value.asInstanceOf[Comparable[Any]].compareTo(0.asInstanceOf[Comparable[Any]])
    }
  }

}

object FlinkRelMdModifiedMonotonicity {

  private val INSTANCE = new FlinkRelMdModifiedMonotonicity

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ModifiedMonotonicityMeta.METHOD, INSTANCE)

}
