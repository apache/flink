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

package org.apache.flink.table.plan.rules.common

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.expressions.{WindowEnd, WindowStart}
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate

import scala.collection.JavaConversions._

object WindowStartEndPropertiesHavingRule {
  private val WINDOW_EXPRESSION_RULE_PREDICATE =
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalFilter],
        RelOptRule.operand(classOf[LogicalProject],
          RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none()))))

  val INSTANCE = new WindowStartEndPropertiesRule("WindowStartEndPropertiesRule" ,
    WINDOW_EXPRESSION_RULE_PREDICATE) {
    override def onMatch(call: RelOptRuleCall): Unit = {

      val project = call.rel(0).asInstanceOf[LogicalProject]
      val filter = call.rel(1).asInstanceOf[LogicalFilter]
      val innerProject = call.rel(2).asInstanceOf[LogicalProject]
      val agg = call.rel(3).asInstanceOf[LogicalWindowAggregate]

      // Retrieve window start and end properties
      val transformed = call.builder()
      transformed.push(LogicalWindowAggregate.create(
        agg.getWindow,
        Seq(
          NamedWindowProperty("w$start", WindowStart(agg.getWindow.aliasAttribute)),
          NamedWindowProperty("w$end", WindowEnd(agg.getWindow.aliasAttribute))
        ), agg)
      )

      // forward window start and end properties
      transformed.project(
        innerProject.getProjects ++ Seq(transformed.field("w$start"), transformed.field("w$end")))

      transformed.filter(filter.getChildExps)

      // replace window auxiliary function by access to window properties
      transformed.project(
        project.getProjects.map(x => replaceGroupAuxiliaries(x, transformed))
      )
      val res = transformed.build()
      call.transformTo(res)
    }

  }
}
