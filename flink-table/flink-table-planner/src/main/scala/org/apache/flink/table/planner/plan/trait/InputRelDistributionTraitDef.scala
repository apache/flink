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

class InputRelDistributionTraitDef extends RelTraitDef[InputRelDistributionTrait] {

  override def getDefault: InputRelDistributionTrait = InputRelDistributionTrait.ANY

  override def getTraitClass: Class[InputRelDistributionTrait] = classOf[InputRelDistributionTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def canConvert(
      planner: RelOptPlanner,
      fromTrait: InputRelDistributionTrait,
      toTrait: InputRelDistributionTrait): Boolean =
    true

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: InputRelDistributionTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {
    throw new RuntimeException("Don't invoke FlinkRelDistributionTraitDef.convert directly!")
  }
}

object InputRelDistributionTraitDef {
  val INSTANCE = new InputRelDistributionTraitDef
}
