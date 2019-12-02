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

import org.apache.flink.table.planner.JBoolean
import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.TableRealRowCountDetection
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecGroupAggregateBase
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.core.{Aggregate, TableScan, Values}
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.util.Util

import scala.collection.JavaConversions._

/**
  * FlinkRelMdTableRealRowCountDetection supplies a implementation of
  * [[FlinkRelMetadataQuery#hasRealRowCount]] for the standard logical algebra.
  */
class FlinkRelMdTableRealRowCountDetection private
  extends MetadataHandler[TableRealRowCountDetection] {

  def getDef: MetadataDef[TableRealRowCountDetection] = FlinkMetadata.TableRealRowCountDetection.DEF

  def hasRealRowCount(rel: TableScan, mq: RelMetadataQuery): JBoolean = {
    val statistic = rel.getTable.asInstanceOf[FlinkPreparingTableBase].getStatistic
    statistic.getRowCount != null
  }

  def hasRealRowCount(rel: Values, mq: RelMetadataQuery): JBoolean = true

  def hasRealRowCount(rel: Aggregate, mq: RelMetadataQuery): JBoolean = {
    hasRealRowCountOfAgg(rel, rel.getGroupSet.toArray, mq)
  }

  def hasRealRowCount(rel: BatchExecGroupAggregateBase, mq: RelMetadataQuery): JBoolean = {
    hasRealRowCountOfAgg(rel, rel.getGrouping, mq)
  }

  def hasRealRowCountOfAgg(
      rel: SingleRel,
      groupSet: Array[Int],
      mq: RelMetadataQuery): JBoolean = {
    if (groupSet.length == 0) {
      // always returns row count: 1
      true
    } else {
      hasRealRowCount(rel.asInstanceOf[RelNode], mq)
    }
  }

  def hasRealRowCount(subset: RelSubset, mq: RelMetadataQuery): JBoolean = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.hasRealRowCount(rel)
  }

  def hasRealRowCount(hepRelVertex: HepRelVertex, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.hasRealRowCount(hepRelVertex.getCurrentRel)
  }

  /**
    * Catch-all rule when none of the others apply.
    */
  def hasRealRowCount(rel: RelNode, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    for (input <- rel.getInputs) {
      val hasRealRowCount = fmq.hasRealRowCount(input)
      if (hasRealRowCount == null || !hasRealRowCount) {
        return hasRealRowCount
      }
    }
    true
  }
}

object FlinkRelMdTableRealRowCountDetection {

  private val INSTANCE = new FlinkRelMdTableRealRowCountDetection

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.TableRealRowCountDetection.METHOD, INSTANCE)

}
