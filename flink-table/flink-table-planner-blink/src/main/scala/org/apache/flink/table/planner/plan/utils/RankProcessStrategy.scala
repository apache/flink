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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.`trait`.TraitUtil
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery

import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

/**
  * Base class of Strategy to choose different rank process function.
  */
sealed trait RankProcessStrategy

case object AppendFastStrategy extends RankProcessStrategy

case object RetractStrategy extends RankProcessStrategy

case class UpdateFastStrategy(primaryKeys: Array[Int]) extends RankProcessStrategy {
  override def toString: String = "UpdateFastStrategy" + primaryKeys.mkString("[", ",", "]")
}

object RankProcessStrategy {

  /**
    * Gets [[RankProcessStrategy]] based on input, partitionKey and orderKey.
    */
  def analyzeRankProcessStrategy(
      input: RelNode,
      partitionKey: ImmutableBitSet,
      orderKey: RelCollation,
      mq: RelMetadataQuery): RankProcessStrategy = {

    val fieldCollations = orderKey.getFieldCollations
    val isUpdateStream = !UpdatingPlanChecker.isAppendOnly(input)

    if (isUpdateStream) {
      val inputIsAccRetract = TraitUtil.isAccRetract(input)
      val uniqueKeys = mq.getUniqueKeys(input)
      if (inputIsAccRetract || uniqueKeys == null || uniqueKeys.isEmpty
        // unique key should contains partition key
        || !uniqueKeys.exists(k => k.contains(partitionKey))) {
        // input is AccRetract or extract the unique keys failed,
        // and we fall back to using retract rank
        RetractStrategy
      } else {
        val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
        val monotonicity = fmq.getRelModifiedMonotonicity(input)
        val isMonotonic = if (monotonicity == null) {
          false
        } else {
          if (fieldCollations.isEmpty) {
            false
          } else {
            fieldCollations.forall { collation =>
              val fieldMonotonicity = monotonicity.fieldMonotonicities(collation.getFieldIndex)
              val direction = collation.direction
              if ((fieldMonotonicity == SqlMonotonicity.DECREASING
                || fieldMonotonicity == SqlMonotonicity.STRICTLY_DECREASING)
                && direction == Direction.ASCENDING) {
                // sort field is ascending and its monotonicity is decreasing
                true
              } else if ((fieldMonotonicity == SqlMonotonicity.INCREASING
                || fieldMonotonicity == SqlMonotonicity.STRICTLY_INCREASING)
                && direction == Direction.DESCENDING) {
                // sort field is descending and its monotonicity is increasing
                true
              } else if (fieldMonotonicity == SqlMonotonicity.CONSTANT) {
                // sort key is a grouping key of upstream agg, it is monotonic
                true
              } else {
                false
              }
            }
          }
        }

        if (isMonotonic) {
          //FIXME choose a set of primary key
          UpdateFastStrategy(uniqueKeys.iterator().next().toArray)
        } else {
          RetractStrategy
        }
      }
    } else {
      AppendFastStrategy
    }
  }
}
