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

import org.apache.flink.table.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.plan.metadata.FlinkMetadata.{ColumnInterval, FlinkDistribution}
import org.apache.flink.table.plan.stats.ValueInterval

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.{JaninoRelMetadataProvider, RelMetadataQuery}

import java.util.function.Supplier

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
  private[this] var distributionHandler: FlinkDistribution.Handler = _

  private def this() {
    this(RelMetadataQuery.THREAD_PROVIDERS.get, RelMetadataQuery.EMPTY)
    this.columnIntervalHandler = RelMetadataQuery.initialHandler(classOf[ColumnInterval.Handler])
    this.distributionHandler = RelMetadataQuery.initialHandler(classOf[FlinkDistribution.Handler])
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

}

object FlinkRelMetadataQuery {

  def instance(): FlinkRelMetadataQuery = new FlinkRelMetadataQuery()

  def traitSet(rel: RelNode): RelTraitSet = {
    rel.getTraitSet.replaceIf(
      FlinkRelDistributionTraitDef.INSTANCE, new Supplier[FlinkRelDistribution]() {
        def get: FlinkRelDistribution = {
          reuseOrCreate(rel.getCluster.getMetadataQuery).flinkDistribution(rel)
        }
      })
  }

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
