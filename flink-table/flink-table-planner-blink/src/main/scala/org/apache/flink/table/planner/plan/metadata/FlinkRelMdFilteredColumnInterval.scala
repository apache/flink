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

import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.FilteredColumnInterval
import org.apache.flink.table.planner.plan.nodes.calcite.TableAggregate
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalGlobalGroupAggregate, StreamPhysicalGroupAggregate, StreamPhysicalGroupTableAggregate, StreamPhysicalGroupWindowAggregate, StreamPhysicalGroupWindowTableAggregate, StreamPhysicalLocalGroupAggregate}
import org.apache.flink.table.planner.plan.stats.ValueInterval
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.sql.`type`.SqlTypeUtil
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * [[FlinkRelMdFilteredColumnInterval]] supplies a default implementation of
  * [[FlinkRelMetadataQuery.getFilteredColumnInterval()]] for the standard logical algebra.
  *
  * The [[FlinkRelMdFilteredColumnInterval]] is almost depend on the implementation of
  * [[FlinkRelMdColumnInterval]], except to handle filter argument in Calc RelNode.
  */
class FlinkRelMdFilteredColumnInterval private extends MetadataHandler[FilteredColumnInterval] {

  override def getDef: MetadataDef[FilteredColumnInterval] =
    FlinkMetadata.FilteredColumnInterval.DEF

  /**
    * Gets the column interval of the given column with filter argument on Project
    *
    * @param project Project RelNode
    * @param mq RelMetadataQuery instance
    * @param columnIndex  the index of the given column
    * @param filterArg  the filter argument
    * @return interval of the given column after filtered in Calc
    */
  def getFilteredColumnInterval(
      project: Project,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val columnInterval = fmq.getColumnInterval(project, columnIndex)
    if (filterArg == -1) {
      columnInterval
    } else {
      val condition = project.getProjects.get(filterArg)
      checkArgument(SqlTypeUtil.inBooleanFamily(condition.getType))
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Option(columnInterval),
        condition,
        columnIndex,
        project.getCluster.getRexBuilder)
    }
  }

  /**
    * Gets the column interval of the given column with filter argument on Filter
    *
    * @param filter Filter RelNode
    * @param mq RelMetadataQuery instance
    * @param columnIndex  the index of the given column
    * @param filterArg  the filter argument
    * @return interval of the given column after filtered in Calc
    */
  def getFilteredColumnInterval(
      filter: Filter,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val columnInterval = fmq.getColumnInterval(filter, columnIndex)
    if (filterArg == -1) {
      columnInterval
    } else {
      // the filter expression is not hold by Filter Node, delegate is to its input
      val filteredColumnInterval = fmq.getFilteredColumnInterval(
        filter.getInput, columnIndex, filterArg)
      // combine filteredInterval and originInterval
      if (filteredColumnInterval == null) {
        columnInterval
      } else {
        val originInterval = if (columnInterval == null) {
          ValueInterval.infinite
        } else {
          columnInterval
        }
        ValueInterval.intersect(originInterval, filteredColumnInterval)
      }
    }
  }

  /**
    * Gets the column interval of the given column with filter argument on Calc
    *
    * @param calc Calc RelNode
    * @param mq RelMetadataQuery instance
    * @param columnIndex  the index of the given column
    * @param filterArg  the filter argument
    * @return interval of the given column after filtered in Calc
    */
  def getFilteredColumnInterval(
      calc: Calc,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val columnInterval = fmq.getColumnInterval(calc, columnIndex)
    if (filterArg == -1) {
      columnInterval
    } else {
      val filterRef = calc.getProgram.getProjectList.get(filterArg)
      val condition = calc.getProgram.expandLocalRef(filterRef)
      checkArgument(SqlTypeUtil.inBooleanFamily(condition.getType))
      ColumnIntervalUtil.getColumnIntervalWithFilter(
        Option(columnInterval),
        condition,
        columnIndex,
        calc.getCluster.getRexBuilder)
    }
  }

  /**
    * Gets the column interval of the given column with filter argument on Exchange
    *
    * @param exchange Exchange RelNode
    * @param mq RelMetadataQuery instance
    * @param columnIndex  the index of the given column
    * @param filterArg  the filter argument
    * @return interval of the given column after filtered in Calc
    */
  def getFilteredColumnInterval(
      exchange: Exchange,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getFilteredColumnInterval(exchange.getInput, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      aggregate: Aggregate,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
    aggregate: TableAggregate,
    mq: RelMetadataQuery,
    columnIndex: Int,
    filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      aggregate: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      aggregate: StreamPhysicalGroupAggregate,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
    aggregate: StreamPhysicalGroupTableAggregate,
    mq: RelMetadataQuery,
    columnIndex: Int,
    filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      aggregate: StreamPhysicalLocalGroupAggregate,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      aggregate: StreamPhysicalGlobalGroupAggregate,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getColumnInterval(
      aggregate: StreamPhysicalGroupWindowAggregate,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
    aggregate: StreamPhysicalGroupWindowTableAggregate,
    mq: RelMetadataQuery,
    columnIndex: Int,
    filterArg: Int): ValueInterval = {
    estimateFilteredColumnIntervalOfAggregate(aggregate, mq, columnIndex, filterArg)
  }

  def estimateFilteredColumnIntervalOfAggregate(
      rel: RelNode,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    checkArgument(filterArg == -1)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getColumnInterval(rel, columnIndex)
  }

  /**
    * Gets the column interval of the given column with filter argument in Union
    *
    * @param union Union RelNode
    * @param mq RelMetadataQuery instance
    * @param columnIndex  the index of the given column
    * @param filterArg  the filter argument
    * @return interval of the given column after filtered in Calc
    */
  def getFilteredColumnInterval(
      union: Union,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val subIntervals = union
      .getInputs
      .map(fmq.getFilteredColumnInterval(_, columnIndex, filterArg))
    subIntervals.reduceLeft(ValueInterval.union)
  }

  def getFilteredColumnInterval(
      subset: RelSubset,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val rel = Util.first(subset.getBest, subset.getOriginal)
    fmq.getFilteredColumnInterval(rel, columnIndex, filterArg)
  }

  def getFilteredColumnInterval(
      rel: RelNode,
      mq: RelMetadataQuery,
      columnIndex: Int,
      filterArg: Int): ValueInterval = null

}

object FlinkRelMdFilteredColumnInterval {

  private val INSTANCE = new FlinkRelMdFilteredColumnInterval

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.FilteredColumnInterval.METHOD, INSTANCE)
}
