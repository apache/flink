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

import org.apache.flink.table.planner.JDouble
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, Rank}
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Join, JoinRelType, Union}
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.{BuiltInMethod, Util}

import scala.collection.JavaConversions._

/**
  * FlinkRelMdPercentageOriginalRows supplies a implementation of
  * [[RelMetadataQuery#getPercentageOriginalRows]] for the standard logical algebra.
  */
class FlinkRelMdPercentageOriginalRows private
  extends MetadataHandler[BuiltInMetadata.PercentageOriginalRows] {

  def getDef: MetadataDef[BuiltInMetadata.PercentageOriginalRows] =
    BuiltInMetadata.PercentageOriginalRows.DEF

  def getPercentageOriginalRows(rel: Expand, mq: RelMetadataQuery): JDouble = {
    mq.getPercentageOriginalRows(rel.getInput)
  }

  def getPercentageOriginalRows(rel: Rank, mq: RelMetadataQuery): JDouble = {
    mq.getPercentageOriginalRows(rel.getInput)
  }

  def getPercentageOriginalRows(rel: Aggregate, mq: RelMetadataQuery): JDouble =
    mq.getPercentageOriginalRows(rel.getInput)

  def getPercentageOriginalRows(
      rel: BatchPhysicalGroupAggregateBase,
      mq: RelMetadataQuery): JDouble = {
    mq.getPercentageOriginalRows(rel.getInput)
  }

  def getPercentageOriginalRows(rel: Join, mq: RelMetadataQuery): JDouble = {
    val left: JDouble = mq.getPercentageOriginalRows(rel.getLeft)
    rel.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        left
      case _ =>
        val right: JDouble = mq.getPercentageOriginalRows(rel.getRight)
        if (left == null || right == null) {
          null
        } else {
          left * right
        }
    }
  }

  def getPercentageOriginalRows(rel: Union, mq: RelMetadataQuery): JDouble = {
    var numerator: JDouble = 0.0
    var denominator: JDouble = 0.0
    rel.getInputs.foreach { input =>
      val inputRowCount = mq.getRowCount(input)
      val percentage = mq.getPercentageOriginalRows(input)
      if (percentage != null && percentage != 0.0) {
        denominator += inputRowCount / percentage
        numerator += inputRowCount
      }
    }
    quotientForPercentage(numerator, denominator)
  }

  def getPercentageOriginalRows(subset: RelSubset, mq: RelMetadataQuery): JDouble = {
    val rel = Util.first(subset.getBest, subset.getOriginal)
    mq.getPercentageOriginalRows(rel)
  }

  /**
    * Catch-all rule when none of the others apply.
    */
  def getPercentageOriginalRows(rel: RelNode, mq: RelMetadataQuery): JDouble = {
    if (rel.getInputs.size > 1) {
      // No generic formula available for multiple inputs.
      return null
    }
    if (rel.getInputs.size == 0) {
      // Assume no filtering happening at leaf.
      return 1.0
    }
    val input = rel.getInput(0)
    val inputPercentage = mq.getPercentageOriginalRows(input)
    if (inputPercentage == null) {
      return null
    }
    // Compute product of percentage filtering from this rel (assuming any
    // filtering is the effect of single-table filters) with the percentage
    // filtering performed by the child.
    val rowCount = mq.getRowCount(rel)
    val inputRowCount = mq.getRowCount(input)
    val relPercentage = quotientForPercentage(rowCount, inputRowCount)
    if (relPercentage == null) {
      return null
    }
    val percent = relPercentage * inputPercentage
    if ((percent < 0.0) || (percent > 1.0)) {
      return null
    }
    relPercentage * inputPercentage
  }

  private def quotientForPercentage(numerator: JDouble, denominator: JDouble): JDouble = {
    if (numerator == null || denominator == null) {
      return null
    }
    // may need epsilon instead
    if (denominator == 0.0) {
      // cap at 100%
      1.0
    } else {
      numerator / denominator
    }
  }
}

object FlinkRelMdPercentageOriginalRows {

  private val INSTANCE = new FlinkRelMdPercentageOriginalRows

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE)

}
