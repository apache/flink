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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamIntersect
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalIntersect

import scala.collection.JavaConverters._

class DataStreamIntersectRule
  extends ConverterRule(
    classOf[FlinkLogicalIntersect],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamIntersectRule"){

  override def convert(rel: RelNode): RelNode = {
    val intersect: FlinkLogicalIntersect = rel.asInstanceOf[FlinkLogicalIntersect]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convLeft: RelNode = RelOptRule.convert(intersect.getInput(0), FlinkConventions.DATASTREAM)
    val convRight: RelNode = RelOptRule.convert(intersect.getInput(1), FlinkConventions.DATASTREAM)

    new DataStreamIntersect(
      rel.getCluster,
      traitSet,
      convLeft,
      convRight,
      rel.getRowType,
      intersect.all)
  }

  override def matches(call: RelOptRuleCall): Boolean = {
    val intersect = call.rel[FlinkLogicalIntersect](0)
    // Check that no event-time attributes are in the input
    // because non-window intersect is unbounded
    // and we don't know how much to hold back watermarks.
    val hasRowtimeFields = intersect.getRowType.getFieldList.asScala
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))
    !hasRowtimeFields
  }
}

object DataStreamIntersectRule {
  val INSTANCE: RelOptRule = new DataStreamIntersectRule
}

