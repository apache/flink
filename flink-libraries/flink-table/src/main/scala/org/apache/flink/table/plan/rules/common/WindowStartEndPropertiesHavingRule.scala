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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalProject}
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate


object WindowStartEndPropertiesHavingRule {
  private val WINDOW_EXPRESSION_RULE_PREDICATE =
    RelOptRule.operand(classOf[LogicalProject],
      RelOptRule.operand(classOf[LogicalFilter],
        RelOptRule.operand(classOf[LogicalProject],
          RelOptRule.operand(classOf[LogicalWindowAggregate], RelOptRule.none()))))

  private val PROJECT_OPERAND_INDEX = 0
  private val INNER_PROJECT_OPERAND_INDEX = 2
  private val LOGICAL_WINDOW_AGGREGATION_OPERAND_INDEX = 3

  val INSTANCE = new WindowStartEndPropertiesRule("WindowStartEndPropertiesRule" ,
    WINDOW_EXPRESSION_RULE_PREDICATE) {

    override private[table] def getProjectOperandIndex =
      PROJECT_OPERAND_INDEX
    override private[table] def getInnerProjectOperandIndex =
      INNER_PROJECT_OPERAND_INDEX
    override private[table] def getLogicalWindowAggregateOperandIndex =
      LOGICAL_WINDOW_AGGREGATION_OPERAND_INDEX
  }
}
