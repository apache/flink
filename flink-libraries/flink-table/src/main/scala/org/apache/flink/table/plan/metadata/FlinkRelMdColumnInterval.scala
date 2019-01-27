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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.metadata.FlinkMetadata.ColumnInterval
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalWindowAggregate, Rank}
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalSnapshot, FlinkLogicalWindowAggregate}
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.nodes.physical.stream._
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.stats._
import org.apache.flink.table.plan.util.FlinkRelOptUtil._
import org.apache.flink.table.plan.util.{ColumnIntervalUtil, ConstantRankRange, FlinkRelMdUtil, VariableRankRange}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{AbstractRelNode, RelNode, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlBinaryOperator, SqlKind}
import org.apache.calcite.util.Util

import java.lang.{Boolean => JBool}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnInterval supplies a default implementation of
  * [[FlinkRelMetadataQuery.getColumnInterval]] for the standard logical algebra.
  */
class FlinkRelMdColumnInterval private extends MetadataHandler[ColumnInterval] {

  override def getDef: MetadataDef[ColumnInterval] = FlinkMetadata.ColumnInterval.DEF

  /**
    * Gets interval of the given column in TableScan.
    *
    * @param ts    TableScan RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in TableScan
    */
  def getColumnInterval(ts: TableScan, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val relOptTable = ts.getTable.asInstanceOf[FlinkRelOptTable]
    val fieldNames = relOptTable.getRowType.getFieldNames
    Preconditions.checkArgument(index >= 0 && index < fieldNames.size())
    val fieldName = fieldNames.get(index)
    val statistic = relOptTable.getFlinkStatistic
    val colStats = statistic.getColumnStats(fieldName)
    if (colStats != null) {
      if (colStats.min == null && colStats.max == null) {
        null
      } else {
        ValueInterval(colStats.min, colStats.max)
      }
    } else {
      null
    }
  }

  /**
    * Gets interval of the given column in FlinkLogicalSnapshot.
    * TODO implements it.
    *
    * @param snapshot    Snapshot RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in TableScan
    */
  def getColumnInterval(
    snapshot: FlinkLogicalSnapshot,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = null

  /**
    * Gets interval of the given column in Project.
    *
    * Note: Only support the simple RexNode, e.g RexInputRef.
    *
    * @param project Project RelNode
    * @param mq      RelMetadataQuery instance
    * @param index   the index of the given column
    * @return interval of the given column in Project
    */
  def getColumnInterval(project: Project, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val projects = project.getProjects
    Preconditions.checkArgument(index >= 0 && index < projects.size())
    projects.get(index) match {
      case inputRef: RexInputRef => fmq.getColumnInterval(project.getInput, inputRef.getIndex)
      case literal: RexLiteral =>
        val literalValue = getLiteralValue(literal)
        if (literalValue == null) {
          ValueInterval.empty
        } else {
          ValueInterval(literalValue, literalValue)
        }
      case rexCall: RexCall if rexCall.op.isInstanceOf[SqlBinaryOperator] =>
        getRexNodeInterval(rexCall, project, mq)
      case _ => null
    }
  }

  /**
    * Gets interval of the given column in Exchange.
    *
    * @param exchange Exchange RelNode
    * @param mq       RelMetadataQuery instance
    * @param index    the index of the given column
    * @return interval of the given column in Exchange
    */
  def getColumnInterval(exchange: Exchange, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(exchange.getInput, index)
  }

  /**
    * Gets interval of the given column in Union.
    *
    * @param union Union RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in Union
    */
  def getColumnInterval(union: Union, mq: RelMetadataQuery, index: Int): ValueInterval =
    estimateColumnIntervalOfUnion(union, mq, index)

  /**
    * Gets interval of the given column in Union.
    *
    * @param union Union RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in batch Union
    */
  private def estimateColumnIntervalOfUnion(
      union: AbstractRelNode,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val subIntervals = union
        .getInputs
        .map(fmq.getColumnInterval(_, index))
    subIntervals.reduceLeft(ValueInterval.union)
  }

  /**
    * Gets interval of the given column in Values.
    *
    * @param values Values RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column in Values
    */
  def getColumnInterval(values: Values, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val tuples = values.tuples
    if (tuples.isEmpty) {
      EmptyValueInterval
    } else {
      val vals = tuples.map(tuple => getLiteralValue(tuple.get(index))).filter(_ != null)
      if (vals.isEmpty) {
        EmptyValueInterval
      } else {
        vals.map(literal => ValueInterval(literal, literal)).reduceLeft(ValueInterval.union)
      }
    }
  }

  /**
    * Gets interval of the given column in Filter.
    *
    * @param filter Filter RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column in Filter
    */
  def getColumnInterval(filter: Filter, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputValueInterval = fmq.getColumnInterval(filter.getInput, index)
    FlinkRelMdColumnInterval.getColumnIntervalWithFilter(
      Option(inputValueInterval),
      filter.getCondition,
      index,
      filter.getCluster.getRexBuilder)
  }

  /**
    * Gets interval of the given column in batch Calc.
    *
    * @param calc  Filter RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in Filter
    */
  def getColumnInterval(calc: Calc, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rexProgram = calc.getProgram
    val project = rexProgram.split().left.get(index)
    getColumnInterval(calc, fmq, project)
  }

  /**
    * Calculate interval of column which results from the given rex node in calc.
    * Note that this function is called by function above, and is reclusive in case
    * of "AS" rex call, and is private, too.
    */
  private def getColumnInterval(
    calc: Calc,
    mq: RelMetadataQuery,
    rex: RexNode): ValueInterval = {

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    rex match {
      case rex: RexCall if rex.getKind == SqlKind.AS =>
        getColumnInterval(calc, fmq, rex.getOperands.head)

      case inputRef: RexInputRef =>
        val rexProgram = calc.getProgram
        val sourceFieldIndex = inputRef.getIndex
        val inputValueInterval = fmq.getColumnInterval(calc.getInput, sourceFieldIndex)
        val condition = rexProgram.getCondition
        if (condition != null) {
          val predicate = rexProgram.expandLocalRef(rexProgram.getCondition)
          FlinkRelMdColumnInterval.getColumnIntervalWithFilter(
            Option(inputValueInterval),
            predicate,
            sourceFieldIndex,
            calc.getCluster.getRexBuilder)
        } else {
          inputValueInterval
        }

      case literal: RexLiteral =>
        val literalValue = getLiteralValue(literal)
        if (literalValue == null) {
          ValueInterval.empty
        } else {
          ValueInterval(literalValue, literalValue)
        }

      case rexCall: RexCall =>
        getRexNodeInterval(rexCall, calc, mq)
      case _ => null
    }
  }

  private def getRexNodeInterval(
    rexNode: RexNode,
    baseNode: SingleRel,
    mq: RelMetadataQuery): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    rexNode match {
      case inputRef: RexInputRef =>
        fmq.getColumnInterval(baseNode.getInput, inputRef.getIndex)

      case literal: RexLiteral =>
        val literalValue = getLiteralValue(literal)
        if (literalValue == null) {
          ValueInterval.empty
        } else {
          ValueInterval(literalValue, literalValue)
        }

      case caseCall: RexCall if caseCall.getKind == SqlKind.CASE =>
        // compute all the possible result values of this case when clause,
        // the result values is the value interval
        val operands = caseCall.getOperands
        val operandCount = operands.size()
        val possibleValueIntervals = operands.indices
          // filter expressions which is condition
          .filter(i => i % 2 != 0 || i == operandCount - 1)
          .map(operands(_))
          .map(getRexNodeInterval(_, baseNode, mq))
        possibleValueIntervals.reduceLeft(ValueInterval.union)

      case ifCall: RexCall if ifCall.getOperator == ScalarSqlFunctions.IF =>
        // compute all the possible result values of this IF clause,
        // the result values is the value interval
        val trueValueInterval = getRexNodeInterval(ifCall.getOperands.get(1), baseNode, mq)
        val falseValueInterval = getRexNodeInterval(ifCall.getOperands.get(2), baseNode, mq)
        ValueInterval.union(trueValueInterval, falseValueInterval)

      case rexCall: RexCall if rexCall.op.isInstanceOf[SqlBinaryOperator] =>
        val leftValueInterval = getRexNodeInterval(rexCall.operands.get(0), baseNode, mq)
        val rightValueInterval = getRexNodeInterval(rexCall.operands.get(1), baseNode, mq)
        ColumnIntervalUtil.getValueIntervalOfRexCall(
          rexCall,
          leftValueInterval,
          rightValueInterval)

      case _ => null
    }
  }

  /**
    * Gets intervals of the given column in Join.
    *
    * @param join  Join RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in Join
    */
  def getColumnInterval(join: Join, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val joinCondition = join.getCondition
    val nLeftColumns = join.getLeft.getRowType.getFieldCount
    val inputValueInterval = if (index < nLeftColumns) {
      fmq.getColumnInterval(join.getLeft, index)
    } else {
      fmq.getColumnInterval(join.getRight, index - nLeftColumns)
    }
    // TODO if column at index position is EuqiJoinKey in a Inner Join, its interval is
    // origin interval intersect interval in the pair joinJoinKey.
    // for example, if join is a InnerJoin with condition l.A = r.A
    // the valueInterval of l.A is the intersect of l.A with r.A
    if (joinCondition == null || joinCondition.isAlwaysTrue) {
      inputValueInterval
    } else {
      FlinkRelMdColumnInterval.getColumnIntervalWithFilter(
        Option(inputValueInterval),
        joinCondition,
        index,
        join.getCluster.getRexBuilder)
    }
  }

  /**
    * Gets intervals of the given column in Aggregates.
    *
    * @param aggregate Aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column in Aggregate
    */
  def getColumnInterval(aggregate: Aggregate, mq: RelMetadataQuery, index: Int): ValueInterval =
    estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets intervals of the given column in FlinkLogicalWindowAggregate.
    *
    * @param agg   Aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in FlinkLogicalWindowAggregate
    */
  def getColumnInterval(
      agg: FlinkLogicalWindowAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets intervals of the given column in LogicalWindowAggregate.
    *
    * @param agg   Aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in LogicalWindowAggregate
    */
  def getColumnInterval(
      agg: LogicalWindowAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets intervals of the given column in WindowAggregateBatchExecBase.
    *
    * @param agg   Aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in LogicalWindowAggregate
    */
  def getColumnInterval(
      agg: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets intervals of the given column in batch OverWindowAggregate.
    *
    * @param aggregate Aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column in batch OverWindowAggregate
    */
  def getColumnInterval(
      aggregate: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = getColumnIntervalOfOverWindow(aggregate, mq, index)

  /**
    * Gets intervals of the given column in calcite window.
    *
    * @param window Window RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column in window
    */
  def getColumnInterval(
      window: Window,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    getColumnIntervalOfOverWindow(window, mq, index)
  }

  private def getColumnIntervalOfOverWindow(
      overWindow: SingleRel,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val input = overWindow.getInput()
    val fieldsCountOfInput = input.getRowType.getFieldCount
    if (index < fieldsCountOfInput) {
      fmq.getColumnInterval(input, index)
    } else {
      // cannot estimate aggregate function calls columnInterval.
      null
    }
  }

  /**
    * Gets intervals of the given column in batch Aggregate.
    *
    * @param aggregate Aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column in batch Aggregate
    */
  def getColumnInterval(
      aggregate: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  def getColumnInterval(
    aggregate: StreamExecGroupAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  def getColumnInterval(
    aggregate: StreamExecLocalGroupAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  def getColumnInterval(
    aggregate: StreamExecGlobalGroupAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    // global aggregate can't estimate the column interval of agg arguments,
    // and the global groupingSet mapping is same to index, so delegate it to local aggregate
    fmq.getColumnInterval(aggregate.getInput, index)
  }

  def getColumnInterval(
    aggregate: StreamExecGroupWindowAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  private def estimateColumnIntervalOfAggregate(
      aggregate: SingleRel,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val groupSet = aggregate match {
      case agg: StreamExecGroupAggregate => agg.getGroupings
      case agg: StreamExecLocalGroupAggregate => agg.getGroupings
      case agg: StreamExecIncrementalGroupAggregate => agg.shuffleKey
      case agg: StreamExecGroupWindowAggregate => agg.getGroupings
      case agg: BatchExecGroupAggregateBase => agg.getGrouping ++ agg.getAuxGrouping
      case agg: Aggregate => checkAndGetFullGroupSet(agg)
      case agg: BatchExecLocalSortWindowAggregate =>
        // grouping + assignTs + auxGrouping
        agg.getGrouping ++ Array(agg.inputTimestampIndex) ++ agg.getAuxGrouping
      case agg: BatchExecLocalHashWindowAggregate =>
        // grouping + assignTs + auxGrouping
        agg.getGrouping ++ Array(agg.inputTimestampIndex) ++ agg.getAuxGrouping
      case agg: BatchExecWindowAggregateBase => agg.getGrouping ++ agg.getAuxGrouping
      // do not match StreamExecGlobalGroupAggregate
    }
    if (index < groupSet.length) {
      // estimates group keys according to the input relNodes.
      val sourceFieldIndex = groupSet(index)
      fmq.getColumnInterval(aggregate.getInput, sourceFieldIndex)
    } else {
      // cannot estimate aggregate function calls columnInterval.
      val aggCallIndex = index - groupSet.length
      val aggregateCall = aggregate match {
        case agg: StreamExecGroupAggregate
          if agg.aggCalls.length > aggCallIndex =>
          agg.aggCalls(aggCallIndex)
        case agg: StreamExecLocalGroupAggregate
          if agg.aggInfoList.getActualAggregateCalls.length > aggCallIndex =>
          agg.aggInfoList.getActualAggregateCalls(aggCallIndex)
        case agg: StreamExecIncrementalGroupAggregate
          if agg.partialAggInfoList.getActualAggregateCalls.length > aggCallIndex =>
          agg.partialAggInfoList.getActualAggregateCalls(aggCallIndex)
        case agg: StreamExecGroupWindowAggregate
          if agg.aggCalls.length > aggCallIndex =>
          agg.aggCalls(aggCallIndex)
        case agg: BatchExecGroupAggregateBase
          if agg.aggregateCalls.length > aggCallIndex =>
          agg.aggregateCalls(aggCallIndex)
        case agg: Aggregate
          if agg.getAggCallList.length > aggCallIndex =>
          agg.getAggCallList.get(aggCallIndex)
        case agg: BatchExecWindowAggregateBase
          if agg.aggregateCalls.length > aggCallIndex =>
          agg.aggregateCalls(aggCallIndex)
        // do not match StreamExecGlobalGroupAggregate
        case _ => null
      }
      if (aggregateCall != null) {
        aggregateCall.getAggregation.getKind match {
          case SUM | SUM0 =>
            val inputInterval: ValueInterval = fmq.getColumnInterval(
              aggregate.getInput,
              aggregateCall.getArgList.get(0))
            if (inputInterval != null) {
              inputInterval match {
                case withLower: WithLower if withLower.lower.isInstanceOf[Number] =>
                  if (withLower.lower.asInstanceOf[Number].doubleValue() >= 0.0) {
                    RightSemiInfiniteValueInterval(withLower.lower, withLower.includeLower)
                  } else {
                    null.asInstanceOf[ValueInterval]
                  }
                case withUpper: WithUpper if withUpper.upper.isInstanceOf[Number] =>
                  if (withUpper.upper.asInstanceOf[Number].doubleValue() <= 0.0) {
                    LeftSemiInfiniteValueInterval(withUpper.upper, withUpper.includeUpper)
                  } else {
                    null
                  }
                case _ => null
              }
            } else {
              null
            }
          case COUNT => RightSemiInfiniteValueInterval(0, includeLower = true)
          // todo add more built-in agg function
          case _ => null
        }
      } else {
        null
      }
    }
  }

  /**
    * Gets intervals of the given column of Sort.
    *
    * @param sort  Sort to analyze
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column in Sort
    */
  def getColumnInterval(sort: Sort, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(sort.getInput, index)
  }

  /**
    * Gets intervals of the given column of Expand.
    *
    * @param expand expand to analyze
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column in batch sort
    */
  def getColumnInterval(
      expand: Expand,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val intervals = expand.projects.flatMap { project =>
      project(index) match {
        case inputRef: RexInputRef =>
          Some(fmq.getColumnInterval(expand.getInput, inputRef.getIndex))
        case l: RexLiteral if l.getTypeName eq SqlTypeName.DECIMAL =>
          val v = l.getValueAs(classOf[java.lang.Long])
          Some(ValueInterval(v, v))
        case l: RexLiteral if l.getValue == null =>
          None
        case p@_ =>
          throw new TableException(s"Column interval can't handle $p type in expand.")
      }
    }
    if (intervals.contains(null)) {
      // null union any value interval is null
      null
    } else {
      intervals.reduce((a, b) => ValueInterval.union(a, b))
    }
  }

  /**
    * Gets intervals of the given column of Rank.
    *
    * @param rank        [[Rank]] instance to analyze
    * @param mq          RelMetadataQuery instance
    * @param index       the index of the given column
    * @return interval of the given column in batch Rank
    */
  def getColumnInterval(
      rank: Rank,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rankFunColumnIndex = FlinkRelMdUtil.getRankFunColumnIndex(rank)
    if (index == rankFunColumnIndex) {
      rank.rankRange match {
        case r: ConstantRankRange => ValueInterval(r.rankStart, r.rankEnd)
        case v: VariableRankRange =>
          val interval = fmq.getColumnInterval(rank.getInput, v.rankEndIndex)
          interval match {
            case hasUpper: WithUpper =>
              val lower = ColumnIntervalUtil.convertStringToNumber("1", hasUpper.upper.getClass)
              lower match {
                case Some(l) =>
                  ValueInterval(l, hasUpper.upper, includeUpper = hasUpper.includeUpper)
                case _ => null
              }
            case _ => null
          }
      }
    } else {
      fmq.getColumnInterval(rank.getInput, index)
    }
  }

  /**
    * Gets intervals of the given column of RelSubset.
    *
    * @param subset RelSubset to analyze
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return If exist best relNode, then transmit to it, else transmit to the original relNode
    */
  def getColumnInterval(subset: RelSubset, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(Util.first(subset.getBest, subset.getOriginal), index)
  }

  /**
    * Catches-all rule when none of the others apply.
    *
    * @param rel   RelNode to analyze
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return Always returns null
    */
  def getColumnInterval(rel: RelNode, mq: RelMetadataQuery, index: Int): ValueInterval = null


}

object FlinkRelMdColumnInterval {

  private val INSTANCE = new FlinkRelMdColumnInterval

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ColumnInterval.METHOD, INSTANCE)


  /**
    * Calculate the interval of column which is referred in predicate expression, and intersect the
    * result with the origin interval of the column.
    *
    * e.g for condition $1 <= 2 and $1 >= -1
    * the interval of $1 is originInterval intersect with [-1, 2]
    *
    * for condition: $1 <= 2 and not ($1 < -1 or $2 is true),
    * the interval of $1 is originInterval intersect with (-Inf, -1]
    *
    * for condition $1 <= 2 or $1 > -1
    * the interval of $1 is (originInterval intersect with (-Inf, 2]) union
    * (originInterval intersect with (-1, Inf])
    *
    * @param originInterval origin interval of the column
    * @param predicate      the predicate expression
    * @param inputRef       the index of the given column
    * @param rexBuilder     RexBuilder instance to analyze the predicate expression
    * @return
    */
  def getColumnIntervalWithFilter(
    originInterval: Option[ValueInterval],
    predicate: RexNode,
    inputRef: Int,
    rexBuilder: RexBuilder): ValueInterval = {

    val isRelated = (r: RexNode)=> r.accept(new ColumnRelatedVisitor(inputRef))
    val relatedSubRexNode = partition(predicate, rexBuilder, isRelated)._1
    val beginInterval = originInterval match {
      case Some(interval) => interval
      case _ => ValueInterval.infinite
    }
    relatedSubRexNode match {
      case Some(rexNode) =>
        val orParts = RexUtil.flattenOr(Vector(RexUtil.toDnf(rexBuilder, rexNode)))
        val interval = orParts.map(or => {
          val andParts = RexUtil.flattenAnd(Vector(or))
          andParts.map(and => columnIntervalOfSinglePredicate(and))
          .filter(_ != null)
          .foldLeft(beginInterval)(ValueInterval.intersect)
        }).reduceLeft(ValueInterval.union)
        if (interval == ValueInterval.infinite) null else interval
      case None => beginInterval
    }

  }

  private def columnIntervalOfSinglePredicate(condition: RexNode): ValueInterval = {
    val convertedCondition = condition.asInstanceOf[RexCall]
    if (convertedCondition == null || convertedCondition.operands.size() != 2) {
      null
    } else {
      val (literalValue, op) = (convertedCondition.operands.head, convertedCondition.operands.last)
      match {
        case (_: RexInputRef, literal: RexLiteral) =>
          (getLiteralValue(literal), convertedCondition.getKind)
        case (rex: RexCall, literal: RexLiteral) if rex.getKind == SqlKind.AS =>
          (getLiteralValue(literal), convertedCondition.getKind)
        case (literal: RexLiteral, _: RexInputRef) =>
          (getLiteralValue(literal), convertedCondition.getKind.reverse())
        case (literal: RexLiteral, rex: RexCall) if rex.getKind == SqlKind.AS =>
          (getLiteralValue(literal), convertedCondition.getKind.reverse())
        case _ => (null, null)
      }
      if (op == null || literalValue == null) {
        null
      } else {
        op match {
          case EQUALS => ValueInterval(literalValue, literalValue)
          case LESS_THAN => ValueInterval(null, literalValue, includeUpper = false)
          case LESS_THAN_OR_EQUAL => ValueInterval(null, literalValue)
          case GREATER_THAN => ValueInterval(literalValue, null, includeLower = false)
          case GREATER_THAN_OR_EQUAL => ValueInterval(literalValue, null)
          case _ => null
        }
      }
    }
  }
}
