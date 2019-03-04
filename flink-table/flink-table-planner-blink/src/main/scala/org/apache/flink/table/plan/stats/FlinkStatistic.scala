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

package org.apache.flink.table.plan.stats

import org.apache.calcite.rel.{RelCollation, RelDistribution, RelReferentialConstraint}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.util.ImmutableBitSet

import java.lang.Double
import java.util

/**
  * The class provides statistics for a [[org.apache.flink.table.plan.schema.FlinkTable]].
  *
  * TODO: Introduce TableStats after https://github.com/apache/flink/pull/7642 finished
  */
class FlinkStatistic extends Statistic {

  /**
    * Returns the number of rows of the table.
    *
    * @return The number of rows of the table.
    */
  override def getRowCount: Double = null

  override def getCollations: util.List[RelCollation] = util.Collections.emptyList()

  /**
    * Returns whether the given columns are a key or a superset of a unique key
    * of this table.
    *
    * Note: Do not call this method!
    * Use [[org.apache.calcite.rel.metadata.RelMetadataQuery]].areRowsUnique if need.
    * Because columns in original uniqueKey may not exist in RowType after project pushDown, however
    * the RowType cannot be available here.
    *
    * @param columns Ordinals of key columns
    * @return if bit mask represents a unique column set; false if not (or
    *         if no metadata is available).
    */
  override def isKey(columns: ImmutableBitSet): Boolean = false

  override def getDistribution: RelDistribution = null

  override def getReferentialConstraints: util.List[RelReferentialConstraint] =
    util.Collections.emptyList()
}

/**
  * Methods to create FlinkStatistic.
  */
object FlinkStatistic {

  /** Represents a FlinkStatistic that knows nothing about a table */
  val UNKNOWN: FlinkStatistic = new FlinkStatistic()

}
