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

package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

/**
  * Definition of the [[UpdateAsRetractionTrait]].
  */
class UpdateAsRetractionTraitDef extends RelTraitDef[UpdateAsRetractionTrait] {
  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: UpdateAsRetractionTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {

    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: UpdateAsRetractionTrait,
      toTrait: UpdateAsRetractionTrait): Boolean = true

  override def getTraitClass: Class[UpdateAsRetractionTrait] = classOf[UpdateAsRetractionTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def getDefault: UpdateAsRetractionTrait = UpdateAsRetractionTrait.DEFAULT
}

object UpdateAsRetractionTraitDef {
  val INSTANCE = new UpdateAsRetractionTraitDef
}

/**
  * Definition of the [[AccModeTrait]].
  */
class AccModeTraitDef extends RelTraitDef[AccModeTrait] {

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: AccModeTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {

    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: AccModeTrait,
      toTrait: AccModeTrait): Boolean = true

  override def getTraitClass: Class[AccModeTrait] = classOf[AccModeTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def getDefault: AccModeTrait = AccModeTrait.UNKNOWN
}

object AccModeTraitDef {
  val INSTANCE = new AccModeTraitDef
}
