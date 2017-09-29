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

package org.apache.flink.table.plan.optimize

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkRuleSets

/**
  * Defines a sequence of programs to optimize batch table plan.
  */
object FlinkBatchPrograms {
  val SUBQUERY = "subquery"
  val TABLE_REF = "table_ref"
  val DECORRELATE = "decorrelate"
  val NORMALIZATION = "normalization"
  val LOGICAL = "logical"
  val PHYSICAL = "physical"

  def buildPrograms(): FlinkChainedPrograms[BatchOptimizeContext] = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()

    // convert sub-queries before query decorrelation
    programs.addLast(
      SUBQUERY,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkRuleSets.TABLE_SUBQUERY_RULES)
        .build())

    // convert table references
    programs.addLast(
      TABLE_REF,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkRuleSets.TABLE_REF_RULES)
        .build())

    // decorrelate
    programs.addLast(DECORRELATE, new FlinkDecorrelateProgram)

    // normalize the logical plan
    programs.addLast(
      NORMALIZATION,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkRuleSets.DATASET_NORM_RULES)
        .build())

    // optimize the logical Flink plan
    programs.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkRuleSets.LOGICAL_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())

    // optimize the physical Flink plan
    programs.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkRuleSets.DATASET_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.DATASET))
        .build())

    programs
  }
}
