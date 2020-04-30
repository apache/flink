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

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata._
import org.apache.calcite.util.BuiltInMethod

import scala.collection.JavaConversions._

/**
  * FlinkRelMdCumulativeCost supplies a implementation of
  * [[RelMetadataQuery#getCumulativeCost]] for the standard logical algebra.
  */
class FlinkRelMdCumulativeCost private extends MetadataHandler[BuiltInMetadata.CumulativeCost] {

  def getDef: MetadataDef[BuiltInMetadata.CumulativeCost] = BuiltInMetadata.CumulativeCost.DEF

  def getCumulativeCost(rel: RelNode, mq: RelMetadataQuery): RelOptCost = {
    val cost = mq.getNonCumulativeCost(rel)
    rel.getInputs.foldLeft(cost) {
      (acc, input) =>
        val inputCost = mq.getCumulativeCost(input)
        acc.plus(inputCost)
    }
  }

  def getCumulativeCost(
      rel: EnumerableInterpreter,
      mq: RelMetadataQuery): RelOptCost = mq.getNonCumulativeCost(rel)

}

object FlinkRelMdCumulativeCost {

  private val INSTANCE = new FlinkRelMdCumulativeCost

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    BuiltInMethod.CUMULATIVE_COST.method, INSTANCE)

}
