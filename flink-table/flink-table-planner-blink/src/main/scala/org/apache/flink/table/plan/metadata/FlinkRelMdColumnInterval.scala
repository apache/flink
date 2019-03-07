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

import org.apache.flink.table.plan.metadata.FlinkMetadata.ColumnInterval
import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.stats._
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.Util

/**
  * FlinkRelMdColumnInterval supplies a implementation of
  * [[FlinkRelMetadataQuery#getColumnInterval]] for the standard logical algebra.
  *
  * <p>[[FlinkRelMdColumnUniqueness]] throws [[RelMdMethodNotImplementedException]]
  * to disable the default implementation on [[RelNode]] and
  * requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
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
      if (colStats.getMinValue == null && colStats.getMaxValue == null) {
        null
      } else {
        ValueInterval(colStats.getMinValue, colStats.getMaxValue)
      }
    } else {
      null
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
  def getColumnInterval(rel: RelNode, mq: RelMetadataQuery, index: Int): ValueInterval = {
    throw RelMdMethodNotImplementedException(
      "getColumnInterval", getClass.getSimpleName, rel.getRelTypeName)
  }

}

object FlinkRelMdColumnInterval {

  private val INSTANCE = new FlinkRelMdColumnInterval

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.ColumnInterval.METHOD, INSTANCE)

}
