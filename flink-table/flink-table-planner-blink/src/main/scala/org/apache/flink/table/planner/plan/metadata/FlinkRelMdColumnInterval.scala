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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.ColumnInterval
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank, TableAggregate, WindowAggregate}
import org.apache.flink.table.planner.plan.nodes.physical.batch._
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.stats._
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ColumnIntervalUtil, FlinkRelOptUtil, RankUtil}
import org.apache.flink.table.runtime.operators.rank.{ConstantRankRange, VariableRankRange}
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.{SqlBinaryOperator, SqlKind}
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdColumnInterval supplies a default implementation of
  * [[FlinkRelMetadataQuery.getColumnInterval]] for the standard logical algebra.
  */
class FlinkRelMdColumnInterval private extends MetadataHandler[ColumnInterval] {

  override def getDef: MetadataDef[ColumnInterval] = FlinkMetadata.ColumnInterval.DEF

  /**
    * Gets interval of the given column on TableScan.
    *
    * @param ts    TableScan RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on TableScan
    */
  def getColumnInterval(ts: TableScan, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val relOptTable = ts.getTable.asInstanceOf[FlinkPreparingTableBase]
    val fieldNames = relOptTable.getRowType.getFieldNames
    Preconditions.checkArgument(index >= 0 && index < fieldNames.size())
    val fieldName = fieldNames.get(index)
    val statistic = relOptTable.getStatistic
    val colStats = statistic.getColumnStats(fieldName)
    if (colStats != null) {
      val minValue = colStats.getMinValue
      val maxValue = colStats.getMaxValue
      val min = colStats.getMin
      val max = colStats.getMax

      Preconditions.checkArgument(
        (minValue == null && maxValue == null) || (max == null && min == null))

      if (minValue != null || maxValue != null) {
        ValueInterval(minValue, maxValue)
      } else if (max != null || min != null) {
        ValueInterval(min, max)
      } else {
        null
      }
    } else {
      null
    }
  }

  /**
    * Gets interval of the given column on Values.
    *
    * @param values Values RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column on Values
    */
  def getColumnInterval(values: Values, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val tuples = values.tuples
    if (tuples.isEmpty) {
      EmptyValueInterval
    } else {
      val values = tuples.map(t => FlinkRelOptUtil.getLiteralValue(t.get(index))).filter(_ != null)
      if (values.isEmpty) {
        EmptyValueInterval
      } else {
        values.map(v => ValueInterval(v, v)).reduceLeft(ValueInterval.union)
      }
    }
  }

  /**
    * Gets interval of the given column on Snapshot.
    *
    * @param snapshot    Snapshot RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on Snapshot.
    */
  def getColumnInterval(snapshot: Snapshot, mq: RelMetadataQuery, index: Int): ValueInterval = null

  /**
    * Gets interval of the given column on Project.
    *
    * Note: Only support the simple RexNode, e.g RexInputRef.
    *
    * @param project Project RelNode
    * @param mq      RelMetadataQuery instance
    * @param index   the index of the given column
    * @return interval of the given column on Project
    */
  def getColumnInterval(project: Project, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val projects = project.getProjects
    Preconditions.checkArgument(index >= 0 && index < projects.size())
    projects.get(index) match {
      case inputRef: RexInputRef => fmq.getColumnInterval(project.getInput, inputRef.getIndex)
      case literal: RexLiteral =>
        val literalValue = FlinkRelOptUtil.getLiteralValue(literal)
        if (literalValue == null) {
          ValueInterval.empty
        } else {
          ValueInterval(literalValue, literalValue)
        }
      case rexCall: RexCall =>
        getRexNodeInterval(rexCall, project, mq)
      case _ => null
    }
  }

  /**
    * Gets interval of the given column on Filter.
    *
    * @param filter Filter RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column on Filter
    */
  def getColumnInterval(filter: Filter, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val inputValueInterval = fmq.getColumnInterval(filter.getInput, index)
    ColumnIntervalUtil.getColumnIntervalWithFilter(
      Option(inputValueInterval),
      filter.getCondition,
      index,
      filter.getCluster.getRexBuilder)
  }

  /**
    * Gets interval of the given column on Calc.
    *
    * @param calc  Filter RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on Calc
    */
  def getColumnInterval(calc: Calc, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rexProgram = calc.getProgram
    val project = rexProgram.split().left.get(index)
    getColumnIntervalOfCalc(calc, fmq, project)
  }

  /**
    * Calculate interval of column which results from the given rex node in calc.
    * Note that this function is called by function above, and is reclusive in case
    * of "AS" rex call, and is private, too.
    */
  private def getColumnIntervalOfCalc(
      calc: Calc,
      mq: RelMetadataQuery,
      project: RexNode): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    project match {
      case call: RexCall if call.getKind == SqlKind.AS =>
        getColumnIntervalOfCalc(calc, fmq, call.getOperands.head)

      case inputRef: RexInputRef =>
        val program = calc.getProgram
        val sourceFieldIndex = inputRef.getIndex
        val inputValueInterval = fmq.getColumnInterval(calc.getInput, sourceFieldIndex)
        val condition = program.getCondition
        if (condition != null) {
          val predicate = program.expandLocalRef(program.getCondition)
          ColumnIntervalUtil.getColumnIntervalWithFilter(
            Option(inputValueInterval),
            predicate,
            sourceFieldIndex,
            calc.getCluster.getRexBuilder)
        } else {
          inputValueInterval
        }

      case literal: RexLiteral =>
        val literalValue = FlinkRelOptUtil.getLiteralValue(literal)
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
        val literalValue = FlinkRelOptUtil.getLiteralValue(literal)
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

      // TODO supports ScalarSqlFunctions.IF
      // TODO supports CAST

      case rexCall: RexCall if rexCall.op.isInstanceOf[SqlBinaryOperator] =>
        val leftValueInterval = getRexNodeInterval(rexCall.operands.get(0), baseNode, mq)
        val rightValueInterval = getRexNodeInterval(rexCall.operands.get(1), baseNode, mq)
        ColumnIntervalUtil.getValueIntervalOfRexCall(rexCall, leftValueInterval, rightValueInterval)

      case _ => null
    }
  }

  /**
    * Gets interval of the given column on Exchange.
    *
    * @param exchange Exchange RelNode
    * @param mq       RelMetadataQuery instance
    * @param index    the index of the given column
    * @return interval of the given column on Exchange
    */
  def getColumnInterval(exchange: Exchange, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(exchange.getInput, index)
  }

  /**
    * Gets interval of the given column on Sort.
    *
    * @param sort  Sort RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on Sort
    */
  def getColumnInterval(sort: Sort, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(sort.getInput, index)
  }

  /**
    * Gets interval of the given column of Expand.
    *
    * @param expand expand RelNode
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
    * Gets interval of the given column on Rank.
    *
    * @param rank        [[Rank]] instance to analyze
    * @param mq          RelMetadataQuery instance
    * @param index       the index of the given column
    * @return interval of the given column on Rank
    */
  def getColumnInterval(
      rank: Rank,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rankFunColumnIndex = RankUtil.getRankNumberColumnIndex(rank).getOrElse(-1)
    if (index == rankFunColumnIndex) {
      rank.rankRange match {
        case r: ConstantRankRange => ValueInterval(r.getRankStart, r.getRankEnd)
        case v: VariableRankRange =>
          val interval = fmq.getColumnInterval(rank.getInput, v.getRankEndIndex)
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
    * Gets interval of the given column on Aggregates.
    *
    * @param aggregate Aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on Aggregate
    */
  def getColumnInterval(aggregate: Aggregate, mq: RelMetadataQuery, index: Int): ValueInterval =
    estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on TableAggregates.
    *
    * @param aggregate TableAggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on TableAggregate
    */
  def getColumnInterval(
      aggregate: TableAggregate,
      mq: RelMetadataQuery, index: Int): ValueInterval =

    estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on batch group aggregate.
    *
    * @param aggregate batch group aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on batch group aggregate
    */
  def getColumnInterval(
      aggregate: BatchExecGroupAggregateBase,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on stream group aggregate.
    *
    * @param aggregate stream group aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on stream group Aggregate
    */
  def getColumnInterval(
      aggregate: StreamExecGroupAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on stream group table aggregate.
    *
    * @param aggregate stream group table aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on stream group TableAggregate
    */
  def getColumnInterval(
    aggregate: StreamExecGroupTableAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on stream local group aggregate.
    *
    * @param aggregate stream local group aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on stream local group Aggregate
    */
  def getColumnInterval(
      aggregate: StreamExecLocalGroupAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on stream global group aggregate.
    *
    * @param aggregate stream global group aggregate RelNode
    * @param mq        RelMetadataQuery instance
    * @param index     the index of the given column
    * @return interval of the given column on stream global group Aggregate
    */
  def getColumnInterval(
      aggregate: StreamExecGlobalGroupAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(aggregate, mq, index)

  /**
    * Gets interval of the given column on window aggregate.
    *
    * @param agg   window aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on window Aggregate
    */
  def getColumnInterval(
      agg: WindowAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets interval of the given column on batch window aggregate.
    *
    * @param agg   batch window aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on batch window Aggregate
    */
  def getColumnInterval(
      agg: BatchExecWindowAggregateBase,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets interval of the given column on stream window aggregate.
    *
    * @param agg   stream window aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on stream window Aggregate
    */
  def getColumnInterval(
      agg: StreamExecGroupWindowAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  /**
    * Gets interval of the given column on stream window table aggregate.
    *
    * @param agg   stream window table aggregate RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on stream window Aggregate
    */
  def getColumnInterval(
    agg: StreamExecGroupWindowTableAggregate,
    mq: RelMetadataQuery,
    index: Int): ValueInterval = estimateColumnIntervalOfAggregate(agg, mq, index)

  private def estimateColumnIntervalOfAggregate(
      aggregate: SingleRel,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val input = aggregate.getInput
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val groupSet = aggregate match {
      case agg: StreamExecGroupAggregate => agg.grouping
      case agg: StreamExecLocalGroupAggregate => agg.grouping
      case agg: StreamExecGlobalGroupAggregate => agg.grouping
      case agg: StreamExecIncrementalGroupAggregate => agg.partialAggGrouping
      case agg: StreamExecGroupWindowAggregate => agg.getGrouping
      case agg: BatchExecGroupAggregateBase => agg.getGrouping ++ agg.getAuxGrouping
      case agg: Aggregate => AggregateUtil.checkAndGetFullGroupSet(agg)
      case agg: BatchExecLocalSortWindowAggregate =>
        // grouping + assignTs + auxGrouping
        agg.getGrouping ++ Array(agg.inputTimeFieldIndex) ++ agg.getAuxGrouping
      case agg: BatchExecLocalHashWindowAggregate =>
        // grouping + assignTs + auxGrouping
        agg.getGrouping ++ Array(agg.inputTimeFieldIndex) ++ agg.getAuxGrouping
      case agg: BatchExecWindowAggregateBase => agg.getGrouping ++ agg.getAuxGrouping
      case agg: TableAggregate => agg.getGroupSet.toArray
      case agg: StreamExecGroupTableAggregate => agg.grouping
      case agg: StreamExecGroupWindowTableAggregate => agg.getGrouping
    }

    if (index < groupSet.length) {
      // estimates group keys according to the input relNodes.
      val sourceFieldIndex = groupSet(index)
      fmq.getColumnInterval(input, sourceFieldIndex)
    } else {
      def getAggCallFromLocalAgg(
          index: Int,
          aggCalls: Seq[AggregateCall],
          inputType: RelDataType): AggregateCall = {
        val outputIndexToAggCallIndexMap = AggregateUtil.getOutputIndexToAggCallIndexMap(
          aggCalls, inputType)
        if (outputIndexToAggCallIndexMap.containsKey(index)) {
          val realIndex = outputIndexToAggCallIndexMap.get(index)
          aggCalls(realIndex)
        } else {
          null
        }
      }

      def getAggCallIndexInLocalAgg(
          index: Int,
          globalAggCalls: Seq[AggregateCall],
          inputRowType: RelDataType): Integer = {
        val outputIndexToAggCallIndexMap = AggregateUtil.getOutputIndexToAggCallIndexMap(
          globalAggCalls, inputRowType)

        outputIndexToAggCallIndexMap.foreach {
          case (k, v) => if (v == index) {
            return k
          }
        }
        null.asInstanceOf[Integer]
      }

      if (index < groupSet.length) {
        // estimates group keys according to the input relNodes.
        val sourceFieldIndex = groupSet(index)
        fmq.getColumnInterval(aggregate.getInput, sourceFieldIndex)
      } else {
        val aggCallIndex = index - groupSet.length
        val aggCall = aggregate match {
          case agg: StreamExecGroupAggregate if agg.aggCalls.length > aggCallIndex =>
            agg.aggCalls(aggCallIndex)
          case agg: StreamExecGlobalGroupAggregate
            if agg.globalAggInfoList.getActualAggregateCalls.length > aggCallIndex =>
            val aggCallIndexInLocalAgg = getAggCallIndexInLocalAgg(
              aggCallIndex, agg.globalAggInfoList.getActualAggregateCalls, agg.inputRowType)
            if (aggCallIndexInLocalAgg != null) {
              return fmq.getColumnInterval(agg.getInput, groupSet.length + aggCallIndexInLocalAgg)
            } else {
              null
            }
          case agg: StreamExecLocalGroupAggregate =>
            getAggCallFromLocalAgg(
              aggCallIndex, agg.aggInfoList.getActualAggregateCalls, agg.getInput.getRowType)
          case agg: StreamExecIncrementalGroupAggregate
            if agg.partialAggInfoList.getActualAggregateCalls.length > aggCallIndex =>
            agg.partialAggInfoList.getActualAggregateCalls(aggCallIndex)
          case agg: StreamExecGroupWindowAggregate if agg.aggCalls.length > aggCallIndex =>
            agg.aggCalls(aggCallIndex)
          case agg: BatchExecLocalHashAggregate =>
            getAggCallFromLocalAgg(aggCallIndex, agg.getAggCallList, agg.getInput.getRowType)
          case agg: BatchExecHashAggregate if agg.isMerge =>
            val aggCallIndexInLocalAgg = getAggCallIndexInLocalAgg(
              aggCallIndex, agg.getAggCallList, agg.aggInputRowType)
            if (aggCallIndexInLocalAgg != null) {
              return fmq.getColumnInterval(agg.getInput, groupSet.length + aggCallIndexInLocalAgg)
            } else {
              null
            }
          case agg: BatchExecLocalSortAggregate =>
            getAggCallFromLocalAgg(aggCallIndex, agg.getAggCallList, agg.getInput.getRowType)
          case agg: BatchExecSortAggregate if agg.isMerge =>
            val aggCallIndexInLocalAgg = getAggCallIndexInLocalAgg(
              aggCallIndex, agg.getAggCallList, agg.aggInputRowType)
            if (aggCallIndexInLocalAgg != null) {
              return fmq.getColumnInterval(agg.getInput, groupSet.length + aggCallIndexInLocalAgg)
            } else {
              null
            }
          case agg: BatchExecGroupAggregateBase if agg.getAggCallList.length > aggCallIndex =>
            agg.getAggCallList(aggCallIndex)
          case agg: Aggregate =>
            val (_, aggCalls) = AggregateUtil.checkAndSplitAggCalls(agg)
            if (aggCalls.length > aggCallIndex) {
              aggCalls(aggCallIndex)
            } else {
              null
            }
          case agg: BatchExecWindowAggregateBase if agg.getAggCallList.length > aggCallIndex =>
            agg.getAggCallList(aggCallIndex)
          case _ => null
        }

        if (aggCall != null) {
          aggCall.getAggregation.getKind match {
            case SUM | SUM0 =>
              val inputInterval = fmq.getColumnInterval(input, aggCall.getArgList.get(0))
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
            // TODO add more built-in agg functions
            case _ => null
          }
        } else {
          null
        }
      }
    }
  }

  /**
    * Gets interval of the given column on calcite window.
    *
    * @param window Window RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return interval of the given column on window
    */
  def getColumnInterval(
      window: Window,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    getColumnIntervalOfOverAgg(window, mq, index)
  }

  /**
    * Gets interval of the given column on batch over aggregate.
    *
    * @param agg    batch over aggregate RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  he index of the given column
    * @return interval of the given column on batch over aggregate.
    */
  def getColumnInterval(
      agg: BatchExecOverAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = getColumnIntervalOfOverAgg(agg, mq, index)

  /**
    * Gets interval of the given column on stream over aggregate.
    *
    * @param agg    stream over aggregate RelNode
    * @param mq     RelMetadataQuery instance
    * @param index  he index of the given column
    * @return interval of the given column on stream over aggregate.
    */
  def getColumnInterval(
      agg: StreamExecOverAggregate,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = getColumnIntervalOfOverAgg(agg, mq, index)

  private def getColumnIntervalOfOverAgg(
      overAgg: SingleRel,
      mq: RelMetadataQuery,
      index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val input = overAgg.getInput
    val fieldsCountOfInput = input.getRowType.getFieldCount
    if (index < fieldsCountOfInput) {
      fmq.getColumnInterval(input, index)
    } else {
      // cannot estimate aggregate function calls columnInterval.
      null
    }
  }

  /**
    * Gets interval of the given column on Join.
    *
    * @param join  Join RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on Join
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
    //TODO if column at index position is EuqiJoinKey in a Inner Join, its interval is
    // origin interval intersect interval in the pair joinJoinKey.
    // for example, if join is a InnerJoin with condition l.A = r.A
    // the valueInterval of l.A is the intersect of l.A with r.A
    if (joinCondition == null || joinCondition.isAlwaysTrue) {
      inputValueInterval
    } else {
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Option(inputValueInterval),
        joinCondition,
        index,
        join.getCluster.getRexBuilder)
    }
  }

  /**
    * Gets interval of the given column on Union.
    *
    * @param union Union RelNode
    * @param mq    RelMetadataQuery instance
    * @param index the index of the given column
    * @return interval of the given column on Union
    */
  def getColumnInterval(union: Union, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val subIntervals = union
      .getInputs
      .map(fmq.getColumnInterval(_, index))
    subIntervals.reduceLeft(ValueInterval.union)
  }

  /**
    * Gets interval of the given column on RelSubset.
    *
    * @param subset RelSubset to analyze
    * @param mq     RelMetadataQuery instance
    * @param index  the index of the given column
    * @return If exist best relNode, then transmit to it, else transmit to the original relNode
    */
  def getColumnInterval(subset: RelSubset, mq: RelMetadataQuery, index: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rel = Util.first(subset.getBest, subset.getOriginal)
    fmq.getColumnInterval(rel, index)
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

}
