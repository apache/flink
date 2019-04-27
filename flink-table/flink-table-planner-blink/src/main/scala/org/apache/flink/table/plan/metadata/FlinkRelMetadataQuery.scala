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

import org.apache.flink.table.JDouble
import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, RelModifiedMonotonicity}
import org.apache.flink.table.plan.metadata.FlinkMetadata._
import org.apache.flink.table.plan.stats.ValueInterval

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}
import org.apache.calcite.util.ImmutableBitSet

/**
  * RelMetadataQuery provides a strongly-typed facade on top of
  * [[org.apache.calcite.rel.metadata.RelMetadataProvider]]
  * for the set of relational expression metadata queries defined as standard within Calcite.
  * FlinkRelMetadataQuery class is to add flink specified metadata queries.
  *
  * @param metadataProvider provider which provides metadata
  * @param prototype        the prototype which provides metadata handlers
  */
class FlinkRelMetadataQuery private(
    metadataProvider: JaninoRelMetadataProvider,
    prototype: RelMetadataQuery) extends RelMetadataQuery(metadataProvider, prototype) {

  private[this] var columnIntervalHandler: ColumnInterval.Handler = _
  private[this] var filteredColumnInterval: FilteredColumnInterval.Handler = _
  private[this] var columnNullCountHandler: ColumnNullCount.Handler = _
  private[this] var columnOriginNullCountHandler: ColumnOriginNullCount.Handler = _
  private[this] var uniqueGroupsHandler: UniqueGroups.Handler = _
  private[this] var distributionHandler: FlinkDistribution.Handler = _
  private[this] var modifiedMonotonicityHandler: ModifiedMonotonicity.Handler = _

  private def this() {
    this(RelMetadataQuery.THREAD_PROVIDERS.get, RelMetadataQuery.EMPTY)
    this.columnIntervalHandler = RelMetadataQuery.initialHandler(classOf[ColumnInterval.Handler])
    this.filteredColumnInterval =
      RelMetadataQuery.initialHandler(classOf[FilteredColumnInterval.Handler])
    this.columnNullCountHandler = RelMetadataQuery.initialHandler(classOf[ColumnNullCount.Handler])
    this.columnOriginNullCountHandler =
      RelMetadataQuery.initialHandler(classOf[ColumnOriginNullCount.Handler])
    this.uniqueGroupsHandler = RelMetadataQuery.initialHandler(classOf[UniqueGroups.Handler])
    this.distributionHandler = RelMetadataQuery.initialHandler(classOf[FlinkDistribution.Handler])
    this.modifiedMonotonicityHandler =
      RelMetadataQuery.initialHandler(classOf[ModifiedMonotonicity.Handler])
  }

  /**
    * Returns the [[ColumnInterval]] statistic.
    *
    * @param rel   the relational expression
    * @param index the index of the given column
    * @return the interval of the given column of a specified relational expression.
    *         Returns null if interval cannot be estimated,
    *         Returns [[org.apache.flink.table.plan.stats.EmptyValueInterval]]
    *         if column values does not contains any value except for null.
    */
  def getColumnInterval(rel: RelNode, index: Int): ValueInterval = {
    try {
      columnIntervalHandler.getColumnInterval(rel, this, index)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        columnIntervalHandler = revise(e.relClass, FlinkMetadata.ColumnInterval.DEF)
        getColumnInterval(rel, index)
    }
  }

  /**
    * Returns the [[ColumnInterval]] of the given column under the given filter argument.
    *
    * @param rel   the relational expression
    * @param columnIndex the index of the given column
    * @param filterArg the index of the filter argument
    * @return the interval of the given column of a specified relational expression.
    *         Returns null if interval cannot be estimated,
    *         Returns [[org.apache.flink.table.plan.stats.EmptyValueInterval]]
    *         if column values does not contains any value except for null.
    */
  def getFilteredColumnInterval(rel: RelNode, columnIndex: Int, filterArg: Int): ValueInterval = {
    try {
      filteredColumnInterval.getFilteredColumnInterval(
        rel, this, columnIndex, filterArg)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        filteredColumnInterval = revise(e.relClass, FlinkMetadata.FilteredColumnInterval.DEF)
        getFilteredColumnInterval(rel, columnIndex, filterArg)
    }
  }

  /**
    * Returns the null count of the given column.
    *
    * @param rel   the relational expression
    * @param index the index of the given column
    * @return the null count of the given column if can be estimated, else return null.
    */
  def getColumnNullCount(rel: RelNode, index: Int): JDouble = {
    try {
      columnNullCountHandler.getColumnNullCount(rel, this, index)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        columnNullCountHandler = revise(e.relClass, FlinkMetadata.ColumnNullCount.DEF)
        getColumnNullCount(rel, index)
    }
  }

  /**
    * Returns origin null count of the given column.
    *
    * @param rel   the relational expression
    * @param index the index of the given column
    * @return the null count of the given column if can be estimated, else return null.
    */
  def getColumnOriginNullCount(rel: RelNode, index: Int): JDouble = {
    try {
      columnOriginNullCountHandler.getColumnOriginNullCount(rel, this, index)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        columnOriginNullCountHandler = revise(e.relClass, FlinkMetadata.ColumnOriginNullCount.DEF)
        getColumnOriginNullCount(rel, index)
    }
  }

  /**
    * Returns the (minimum) unique groups of the given columns.
    *
    * @param rel the relational expression
    * @param columns the given columns in a specified relational expression.
    *                The given columns should not be null.
    * @return the (minimum) unique columns which should be a sub-collection of the given columns,
    *         and should not be null or empty. If none unique columns can be found, return the
    *         given columns.
    */
  def getUniqueGroups(rel: RelNode, columns: ImmutableBitSet): ImmutableBitSet = {
    try {
      require(columns != null)
      if (columns.isEmpty) {
        return columns
      }
      val uniqueGroups = uniqueGroupsHandler.getUniqueGroups(rel, this, columns)
      require(uniqueGroups != null && !uniqueGroups.isEmpty)
      require(columns.contains(uniqueGroups))
      uniqueGroups
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        uniqueGroupsHandler = revise(e.relClass, FlinkMetadata.UniqueGroups.DEF)
        getUniqueGroups(rel, columns)
    }
  }

  /**
    * Returns the [[FlinkRelDistribution]] statistic.
    *
    * @param rel the relational expression
    * @return description of how the rows in the relational expression are
    *         physically distributed
    */
  def flinkDistribution(rel: RelNode): FlinkRelDistribution = {
    try {
      distributionHandler.flinkDistribution(rel, this)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        distributionHandler = revise(e.relClass, FlinkMetadata.FlinkDistribution.DEF)
        flinkDistribution(rel)
    }
  }

  /**
    * Returns the [[RelModifiedMonotonicity]] statistic.
    *
    * @param rel the relational expression
    * @return the monotonicity for the corresponding RelNode
    */
  def getRelModifiedMonotonicity(rel: RelNode): RelModifiedMonotonicity = {
    try {
      modifiedMonotonicityHandler.getRelModifiedMonotonicity(rel, this)
    } catch {
      case e: JaninoRelMetadataProvider.NoHandler =>
        modifiedMonotonicityHandler = revise(e.relClass, FlinkMetadata.ModifiedMonotonicity.DEF)
        getRelModifiedMonotonicity(rel)
    }
  }

}

object FlinkRelMetadataQuery {

  def instance(): FlinkRelMetadataQuery = new FlinkRelMetadataQuery()

  /**
    * Reuse input metadataQuery instance if it could cast to FlinkRelMetadataQuery class,
    * or create one if not.
    *
    * @param mq metadataQuery which try to reuse
    * @return a FlinkRelMetadataQuery instance
    */
  def reuseOrCreate(mq: RelMetadataQuery): FlinkRelMetadataQuery = {
    mq match {
      case q: FlinkRelMetadataQuery => q
      case _ => FlinkRelMetadataQuery.instance()
    }
  }
}
