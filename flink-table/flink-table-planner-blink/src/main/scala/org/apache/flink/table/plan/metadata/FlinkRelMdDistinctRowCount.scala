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

import org.apache.flink.table.plan.schema.FlinkRelOptTable
import org.apache.flink.table.plan.util.FlinkRelMdUtil

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rex._
import org.apache.calcite.util._

import java.lang.Double

import scala.collection.JavaConversions._

/**
  * FlinkRelMdDistinctRowCount supplies a implementation of
  * [[RelMetadataQuery#getDistinctRowCount]] for the standard logical algebra.
  *
  * <p>Different from Calcite's default implementation, [[FlinkRelMdDistinctRowCount]] throws
  * [[RelMdMethodNotImplementedException]] to disable the default implementation on [[RelNode]]
  * and requires to provide implementation on each kind of RelNode.
  *
  * <p>When add a new kind of RelNode, the author maybe forget to implement the logic for
  * the new rel, and get the unexpected result from the method on [[RelNode]]. So this handler
  * will force the author to implement the logic for new rel to avoid the unexpected result.
  */
class FlinkRelMdDistinctRowCount private extends MetadataHandler[BuiltInMetadata.DistinctRowCount] {

  def getDef: MetadataDef[BuiltInMetadata.DistinctRowCount] = BuiltInMetadata.DistinctRowCount.DEF

  def getDistinctRowCount(
      rel: TableScan,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): Double = {
    if (predicate == null || predicate.isAlwaysTrue) {
      if (groupKey.isEmpty) {
        return 1D
      }
    }
    val statistic = rel.getTable.asInstanceOf[FlinkRelOptTable].getFlinkStatistic
    val fields = rel.getRowType.getFieldList
    val isUniqueKey = mq.areColumnsUnique(rel, groupKey)
    val isUnique = isUniqueKey != null && isUniqueKey
    val selectivity: Double = if (predicate == null) {
      1D
    } else {
      mq.getSelectivity(rel, predicate)
    }
    if (isUnique) {
      val rowCount = mq.getRowCount(rel)
      NumberUtil.multiply(rowCount, selectivity)
    } else {
      val distinctCount = groupKey.asList().foldLeft(1D) {
        (ndv, g) =>
          val fieldName = fields.get(g).getName
          val colStats = statistic.getColumnStats(fieldName)
          if (colStats != null && colStats.getNdv != null) {
            // Never let ndv of a column go below 1, as it will result in incorrect calculations.
            ndv * Math.max(colStats.getNdv.toDouble, 1D)
          } else {
            return null
          }
      }
      val rowCount = mq.getRowCount(rel)
      FlinkRelMdUtil.adaptNdvBasedOnSelectivity(rowCount, distinctCount, selectivity)
    }
  }

  def getDistinctRowCount(
      subset: RelSubset,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): Double = {
    if (!Bug.CALCITE_1048_FIXED) {
      val rel = Util.first(subset.getBest, subset.getOriginal)
      return mq.getDistinctRowCount(rel, groupKey, predicate)
    }

    subset.getRels.foldLeft(null.asInstanceOf[Double]) {
      (min, input) =>
        try {
          val inputDistinctRowCount = mq.getDistinctRowCount(input, groupKey, predicate)
          NumberUtil.min(min, inputDistinctRowCount)
        } catch {
          // Ignore this relational expression; there will be non-cyclic ones
          // in this set.
          case e: CyclicMetadataException => min
        }
    }
  }

  /**
    * Throws [[RelMdMethodNotImplementedException]] to
    * force implement [[getDistinctRowCount]] logic on each kind of RelNode.
    */
  def getDistinctRowCount(
      rel: RelNode,
      mq: RelMetadataQuery,
      groupKey: ImmutableBitSet,
      predicate: RexNode): Double = {
    throw RelMdMethodNotImplementedException(
      "getDistinctRowCount", getClass.getSimpleName, rel.getRelTypeName)
  }

}

object FlinkRelMdDistinctRowCount {

  private val INSTANCE = new FlinkRelMdDistinctRowCount

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE)

}
