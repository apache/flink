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

import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction
import org.apache.flink.table.planner.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.ModifiedMonotonicity
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, TableAggregate, WindowAggregate, WindowTableAggregate}
import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchPhysicalCorrelate, BatchPhysicalGroupAggregateBase}
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, TableSourceTable}
import org.apache.flink.table.planner.plan.stats.{WithLower, WithUpper}
import org.apache.flink.table.planner.{JByte, JDouble, JFloat, JList, JLong, JShort}
import org.apache.flink.types.RowKind

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelCollation, RelFieldCollation, RelNode}
import org.apache.calcite.rex.{RexCall, RexCallBinding, RexInputRef, RexNode}
import org.apache.calcite.sql.fun.{SqlCountAggFunction, SqlMinMaxAggFunction, SqlSumAggFunction, SqlSumEmptyIsZeroAggFunction}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlMonotonicity._
import org.apache.calcite.sql.{SqlKind, SqlOperatorBinding}
import org.apache.calcite.util.Util

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}
import java.util.Collections

import scala.collection.JavaConversions._

/**
 * FlinkRelMdModifiedMonotonicity supplies a default implementation of
 * [[FlinkRelMetadataQuery#getRelModifiedMonotonicity]] for logical algebra.
 */
class FlinkRelMdModifiedMonotonicity private extends MetadataHandler[ModifiedMonotonicity] {

  override def getDef: MetadataDef[ModifiedMonotonicity] = FlinkMetadata.ModifiedMonotonicity.DEF

  def getRelModifiedMonotonicity(rel: TableScan, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val monotonicity: RelModifiedMonotonicity = rel match {
      case _: FlinkLogicalDataStreamTableScan | _: StreamPhysicalDataStreamScan =>
        val table = rel.getTable.unwrap(classOf[FlinkPreparingTableBase])
        table.getStatistic.getRelModifiedMonotonicity
      case _: FlinkLogicalTableSourceScan | _: StreamPhysicalTableSourceScan =>
        val table = rel.getTable.unwrap(classOf[TableSourceTable])
        table.tableSource match {
          case sts: ScanTableSource if !sts.getChangelogMode.containsOnly(RowKind.INSERT) =>
            // changelog source can't produce CONSTANT ModifiedMonotonicity
            new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(NOT_MONOTONIC))
          case _ => null
        }
      case _ => null
    }

    if (monotonicity != null) {
      monotonicity
    } else {
      new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(CONSTANT))
    }
  }


  def getRelModifiedMonotonicity(rel: Project, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getProjectMonotonicity(rel.getProjects, rel.getInput, mq)
  }

  def getRelModifiedMonotonicity(rel: Calc, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val projects = rel.getProgram.getProjectList.map(rel.getProgram.expandLocalRef)
    getProjectMonotonicity(projects, rel.getInput, mq)
  }

  private def getProjectMonotonicity(
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
    // init field monotonicities
    val fieldMonotonicities = Array.fill(projects.size())(NOT_MONOTONIC)
    val inputFieldMonotonicities = fmq.getRelModifiedMonotonicity(input).fieldMonotonicities

    def getInputFieldIndex(node: RexNode, indexInProject: Int): Int = {
      node match {
        case ref: RexInputRef =>
          fieldMonotonicities(indexInProject) = inputFieldMonotonicities(ref.getIndex)
          ref.getIndex
        case a: RexCall if a.getKind == SqlKind.AS || a.getKind == SqlKind.CAST =>
          getInputFieldIndex(a.getOperands.get(0), indexInProject)
        case c: RexCall if c.getOperands.size() == 1 =>
          c.getOperator match {
            case ssf: ScalarSqlFunction =>
              val inputIndex = getInputFieldIndex(c.getOperands.get(0), indexInProject)
              // collations of stream node are empty currently.
              val binding = RexCallBinding.create(
                input.getCluster.getTypeFactory, c, Collections.emptyList[RelCollation])
              val udfMonotonicity = getUdfMonotonicity(ssf, binding)
              val inputMono = if (inputIndex > -1) {
                inputFieldMonotonicities(inputIndex)
              } else {
                NOT_MONOTONIC
              }
              if (inputMono == udfMonotonicity) {
                fieldMonotonicities(indexInProject) = inputMono
              } else {
                fieldMonotonicities(indexInProject) = NOT_MONOTONIC
              }
              inputIndex
            case _ => -1
          }
        case _ => -1
      }
    }

    // copy child mono
    projects.zipWithIndex.foreach { case (expr, idx) =>
      getInputFieldIndex(expr, idx)
    }
    new RelModifiedMonotonicity(fieldMonotonicities)
  }

  def getRelModifiedMonotonicity(rel: Expand, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput(0), mq, rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(rel: Rank, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputMonotonicity = fmq.getRelModifiedMonotonicity(rel.getInput)

    // If child monotonicity is null, we should return early.
    if (inputMonotonicity == null) {
      return null
    }

    // if partitionBy a update field or partitionBy a field whose mono is null, just return null
    if (rel.partitionKey.exists(e => inputMonotonicity.fieldMonotonicities(e) != CONSTANT)) {
      return null
    }

    val fieldCount = rel.getRowType.getFieldCount

    // init current mono
    val currentMonotonicity = notMonotonic(fieldCount)
    // 1. partitionBy field is CONSTANT
    rel.partitionKey.foreach(e => currentMonotonicity.fieldMonotonicities(e) = CONSTANT)
    // 2. row number filed is CONSTANT
    if (rel.outputRankNumber) {
      currentMonotonicity.fieldMonotonicities(fieldCount - 1) = CONSTANT
    }
    // 3. time attribute field is increasing
    (0 until fieldCount).foreach(e => {
      if (FlinkTypeFactory.isTimeIndicatorType(rel.getRowType.getFieldList.get(e).getType)) {
        inputMonotonicity.fieldMonotonicities(e) = INCREASING
      }
    })
    val fieldCollations = rel.orderKey.getFieldCollations
    if (fieldCollations.nonEmpty) {
      // 4. process the first collation field, we can only deduce the first collation field
      val firstCollation = fieldCollations.get(0)
      // Collation field index in child node will be same with Rank node,
      // see ProjectToLogicalProjectAndWindowRule for details.
      val fieldMonotonicity = inputMonotonicity.fieldMonotonicities(firstCollation.getFieldIndex)
      val result = fieldMonotonicity match {
        case SqlMonotonicity.INCREASING | SqlMonotonicity.CONSTANT
          if firstCollation.direction == RelFieldCollation.Direction.DESCENDING => INCREASING
        case SqlMonotonicity.DECREASING | SqlMonotonicity.CONSTANT
          if firstCollation.direction == RelFieldCollation.Direction.ASCENDING => DECREASING
        case _ => NOT_MONOTONIC
      }
      currentMonotonicity.fieldMonotonicities(firstCollation.getFieldIndex) = result
    }

    currentMonotonicity
  }

  def getRelModifiedMonotonicity(
      rel: StreamExecDeduplicate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    if (allAppend(mq, rel.getInput)) {
      val mono = new RelModifiedMonotonicity(
        Array.fill(rel.getRowType.getFieldCount)(NOT_MONOTONIC))
      rel.getUniqueKeys.foreach(e => mono.fieldMonotonicities(e) = CONSTANT)
      mono
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalChangelogNormalize,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val mono = new RelModifiedMonotonicity(Array.fill(rel.getRowType.getFieldCount)(NOT_MONOTONIC))
    rel.uniqueKeys.foreach(e => mono.fieldMonotonicities(e) = CONSTANT)
    mono
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalDropUpdateBefore,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput, mq, rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalWatermarkAssigner,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput, mq, rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalMiniBatchAssigner,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput, mq, rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(rel: Exchange, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    // for exchange, get correspond from input
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(rel.getInput)
  }

  def getRelModifiedMonotonicity(rel: Aggregate, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnAggregate(rel.getInput, mq, rel.getAggCallList.toList,
      rel.getGroupSet.toArray)
  }

  def getRelModifiedMonotonicity(
      rel: WindowTableAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    if (allAppend(mq, rel.getInput)) {
      constants(rel.getRowType.getFieldCount)
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
      rel: TableAggregate, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnTableAggregate(
      rel.getInput, rel.getGroupSet.toArray, rel.getRowType.getFieldCount, mq)
  }

  def getRelModifiedMonotonicity(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery): RelModifiedMonotonicity = null

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalGroupAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnAggregate(rel.getInput, mq, rel.aggCalls.toList, rel.grouping)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalGroupTableAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnTableAggregate(
      rel.getInput, rel.grouping, rel.getRowType.getFieldCount, mq)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalGlobalGroupAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    // global and local agg should have same update monotonicity
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(rel.getInput)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalLocalGroupAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnAggregate(rel.getInput, mq, rel.aggCalls.toList, rel.grouping)
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalIncrementalGroupAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getRelModifiedMonotonicityOnAggregate(
      rel.getInput, mq, rel.finalAggCalls.toList, rel.finalAggGrouping)
  }

  def getRelModifiedMonotonicity(
      rel: WindowAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = null

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalGroupWindowAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    if (allAppend(mq, rel.getInput) && !rel.emitStrategy.produceUpdates) {
      constants(rel.getRowType.getFieldCount)
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalGroupWindowTableAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    if (allAppend(mq, rel.getInput)) {
      constants(rel.getRowType.getFieldCount)
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
      rel: FlinkLogicalOverAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = constants(rel.getRowType.getFieldCount)

  def getRelModifiedMonotonicity(
      rel: StreamExecOverAggregate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = constants(rel.getRowType.getFieldCount)

  def getRelModifiedMonotonicityOnTableAggregate(
      input: RelNode,
      grouping: Array[Int],
      rowSize: Int,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputMonotonicity = fmq.getRelModifiedMonotonicity(input)

    // if group by an update field or group by a field mono is null, just return null
    if (inputMonotonicity == null ||
        grouping.exists(e => inputMonotonicity.fieldMonotonicities(e) != CONSTANT)) {
      return null
    }

    val groupCnt = grouping.length
    val fieldMonotonicity =
      Array.fill(groupCnt)(CONSTANT) ++ Array.fill(rowSize - grouping.length)(NOT_MONOTONIC)
    new RelModifiedMonotonicity(fieldMonotonicity)
  }

  def getRelModifiedMonotonicityOnAggregate(
      input: RelNode,
      mq: RelMetadataQuery,
      aggCallList: List[AggregateCall],
      grouping: Array[Int]): RelModifiedMonotonicity = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputMonotonicity = fmq.getRelModifiedMonotonicity(input)

    // if group by a update field or group by a field mono is null, just return null
    if (inputMonotonicity == null ||
        grouping.exists(e => inputMonotonicity.fieldMonotonicities(e) != CONSTANT)) {
      return null
    }

    val groupCnt = grouping.length
    // init monotonicity for group keys and agg calls
    val fieldMonotonicities =
      Array.fill(groupCnt)(CONSTANT) ++ Array.fill(aggCallList.size)(NOT_MONOTONIC)

    // get original monotonicity ignore input
    aggCallList.zipWithIndex.foreach { case (aggCall, idx) =>
      val aggCallMonotonicity = getMonotonicityOnAggCall(aggCall, fmq, input)
      fieldMonotonicities(idx + groupCnt) = aggCallMonotonicity
    }

    // need to re-calculate monotonicity if child contains update
    if (containsUpdate(fmq, input)) {
      aggCallList.zipWithIndex.foreach { case (aggCall, idx) =>
        val index = groupCnt + idx
        if (aggCall.getArgList.size() > 1) {
          fieldMonotonicities(index) = NOT_MONOTONIC
        } else if (aggCall.getArgList.size() == 1) {
          val childMono = inputMonotonicity.fieldMonotonicities(aggCall.getArgList.head)
          val currentMono = fieldMonotonicities(index)
          if (childMono != currentMono &&
              !aggCall.getAggregation.isInstanceOf[SqlCountAggFunction]) {
            // count will Increasing even child is NOT_MONOTONIC
            fieldMonotonicities(index) = NOT_MONOTONIC
          }
        }
      }
    }
    new RelModifiedMonotonicity(fieldMonotonicities)
  }

  def getMonotonicityOnAggCall(
      aggCall: AggregateCall,
      mq: RelMetadataQuery,
      input: RelNode): SqlMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    aggCall.getAggregation match {
      case _: SqlCountAggFunction => INCREASING
      case minMax: SqlMinMaxAggFunction => minMax.kind match {
        case SqlKind.MAX => INCREASING
        case SqlKind.MIN => DECREASING
        case _ => NOT_MONOTONIC
      }
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

  def getRelModifiedMonotonicity(rel: Join, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val joinType = rel.getJoinType
    if (joinType.equals(JoinRelType.ANTI)) {
      return null
    }

    val left = rel.getLeft
    val right = rel.getRight
    val joinInfo = rel.analyzeCondition
    val leftKeys = joinInfo.leftKeys
    val rightKeys = joinInfo.rightKeys
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)

    // if group set contains update return null
    val containDelete = containsDelete(fmq, left) || containsDelete(fmq, right)
    val containUpdate = containsUpdate(fmq, left) || containsUpdate(fmq, right)

    def isAllConstantOnKeys(rel: RelNode, keys: Array[Int]): Boolean = {
      val mono = fmq.getRelModifiedMonotonicity(rel)
      keys.forall(mono != null && mono.fieldMonotonicities(_) == CONSTANT)
    }

    val isKeyAllAppend = isAllConstantOnKeys(left, leftKeys.toIntArray) &&
      isAllConstantOnKeys(right, rightKeys.toIntArray)

    if (!containDelete && isKeyAllAppend && (containUpdate && joinInfo.isEqui || !containUpdate)) {

      // output rowtype of semi equals to the rowtype of left child
      if (joinType.equals(JoinRelType.SEMI)) {
        fmq.getRelModifiedMonotonicity(left)
      } else {
        val leftFieldMonotonicities = fmq.getRelModifiedMonotonicity(left).fieldMonotonicities
        val rightFieldMonotonicities = fmq.getRelModifiedMonotonicity(right).fieldMonotonicities
        new RelModifiedMonotonicity(leftFieldMonotonicities ++ rightFieldMonotonicities)
      }
    } else {
      null
    }
  }

  def getRelModifiedMonotonicity(
      rel: StreamExecIntervalJoin,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    // window join won't have update
    constants(rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(rel: Correlate, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput(0), mq, rel.getRowType.getFieldCount)
  }

  def getRelModifiedMonotonicity(
      rel: BatchPhysicalCorrelate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = null

  def getRelModifiedMonotonicity(
      rel: StreamPhysicalCorrelate,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    getMonotonicity(rel.getInput(0), mq, rel.getRowType.getFieldCount)
  }

  // TODO supports temporal table function join

  def getRelModifiedMonotonicity(rel: Union, mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (rel.getInputs.exists(p => containsDelete(fmq, p))) {
      null
    } else {
      val inputMonotonicities = rel.getInputs.map(fmq.getRelModifiedMonotonicity)
      val head = inputMonotonicities.head
      if (inputMonotonicities.forall(head.equals(_))) {
        head
      } else {
        notMonotonic(rel.getRowType.getFieldCount)
      }
    }
  }

  def getRelModifiedMonotonicity(
      hepRelVertex: HepRelVertex,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(hepRelVertex.getCurrentRel)
  }

  def getRelModifiedMonotonicity(
      subset: RelSubset,
      mq: RelMetadataQuery): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rel = Util.first(subset.getBest, subset.getOriginal)
    fmq.getRelModifiedMonotonicity(rel)
  }

  def getRelModifiedMonotonicity(rel: RelNode, mq: RelMetadataQuery): RelModifiedMonotonicity = null

  /**
   * Utility to create a RelModifiedMonotonicity which all fields is modified constant which
   * means all the field's value will not be modified.
   */
  def constants(fieldCount: Int): RelModifiedMonotonicity = {
    new RelModifiedMonotonicity(Array.fill(fieldCount)(CONSTANT))
  }

  def notMonotonic(fieldCount: Int): RelModifiedMonotonicity = {
    new RelModifiedMonotonicity(Array.fill(fieldCount)(NOT_MONOTONIC))
  }

  /**
   * These operator won't generate update itself
   */
  def getMonotonicity(
      input: RelNode,
      mq: RelMetadataQuery,
      fieldCount: Int): RelModifiedMonotonicity = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (containsDelete(fmq, input)) {
      null
    } else if (allAppend(fmq, input)) {
      new RelModifiedMonotonicity(Array.fill(fieldCount)(CONSTANT))
    } else {
      new RelModifiedMonotonicity(Array.fill(fieldCount)(NOT_MONOTONIC))
    }
  }

  def containsDelete(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getRelModifiedMonotonicity(node) == null
  }

  def containsUpdate(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (containsDelete(fmq, node)) {
      false
    } else {
      val monotonicity = fmq.getRelModifiedMonotonicity(node)
      monotonicity.fieldMonotonicities.exists(_ != CONSTANT)
    }
  }

  def allAppend(mq: RelMetadataQuery, node: RelNode): Boolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    if (containsDelete(fmq, node)) {
      false
    } else {
      val monotonicity = fmq.getRelModifiedMonotonicity(node)
      monotonicity.fieldMonotonicities.forall(_ == CONSTANT)
    }
  }

  def getUdfMonotonicity(udf: ScalarSqlFunction, binding: SqlOperatorBinding): SqlMonotonicity = {
    // get monotonicity info from ScalarSqlFunction directly.
    udf.getMonotonicity(binding)
  }

  private def isValueGreaterThanZero[T](value: Comparable[T]): Int = {
    value match {
      case i: Integer => i.compareTo(0)
      case l: JLong => l.compareTo(0L)
      case db: JDouble => db.compareTo(0d)
      case f: JFloat => f.compareTo(0f)
      case s: JShort => s.compareTo(0.toShort)
      case b: JByte => b.compareTo(0.toByte)
      case dec: JBigDecimal => dec.compareTo(JBigDecimal.ZERO)
      case _: Date | _: Time | _: Timestamp | _: String =>
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
    FlinkMetadata.ModifiedMonotonicity.METHOD, INSTANCE)

}
