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

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.rules.FlinkBatchExecRuleSets
import org.apache.calcite.plan.hep.HepMatchOrder

/**
  * Defines a sequence of programs to optimize flink batch exec table plan.
  */
object FlinkBatchPrograms {

  val SUBQUERY_REWRITE = "subquery_rewrite"
  val CORRELATE_REWRITE = "correlate_rewrite"
  val DECORRELATE = "decorrelate"
  val DEFAULT_REWRITE = "default_rewrite"
  val PREDICATE_PUSHDOWN = "predicate_pushdown"
  val JOIN_REORDER = "join_reorder"
  val JOIN_REWRITE = "join_rewrite"
  val WINDOW = "window"
  val LOGICAL = "logical"
  val LOGICAL_REWRITE = "logical_rewrite"
  val PHYSICAL = "physical"
  val PHYSICAL_REWRITE = "physical_rewrite"

  def buildPrograms(config: Configuration): FlinkChainedPrograms[BatchOptimizeContext] = {
    val programs = new FlinkChainedPrograms[BatchOptimizeContext]()

    programs.addLast(
      // rewrite sub-queries to joins
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        // rewrite RelTable before rewriting sub-queries
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .build()
    )

    // rewrite special temporal join plan
    programs.addLast(
      CORRELATE_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.EXPAND_PLAN_RULES)
          .build(), "convert correlate to temporal table join")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.POST_EXPAND_CLEAN_UP_RULES)
          .build(), "convert enumerable table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.UNNEST_RULES)
          .build(), "convert unnest into table function scan")
        .build())

    // query decorrelation
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    // default rewrite, includes: predicate simplification, expression reduction, window
    // properties rewrite, etc.
    programs.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_DEFAULT_REWRITE_RULES)
        .build())

    // rule based optimization: push down predicate(s) include join predicate and/or where clause
    // so it only needs to read the required data
    programs.addLast(
      PREDICATE_PUSHDOWN,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkBatchExecRuleSets.BATCH_EXEC_JOIN_PREDICATE_REWRITE_RULES)
                .build(), "join predicate rewrite")
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkBatchExecRuleSets.FILTER_PREPARE_RULES)
                .build(), "other predicate rewrite")
            .setIterations(5).build())
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.FILTER_TABLESCAN_PUSHDOWN_RULES)
            .build(), "predicate push into scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.PRUNE_EMPTY_RULES)
            .build(), "prune empty after predicate push down")
        .build())

    // join reorder
    if (config.getBoolean(TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED)) {
      programs.addLast(
        JOIN_REORDER,
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.BATCH_EXEC_JOIN_REORDER)
          .build())
    }

    // join rewrite
    programs.addLast(
      JOIN_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.SKEW_JOIN_REWRITE_RULES)
            .build(), "skewed join rewrite")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.JOIN_COND_EQUAL_TRANSFER_RULES)
            .build(), "join condition transitive closure")
        .build())

    // window rewrite
    programs.addLast(
      WINDOW,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.PROJECT_RULES)
            .build(), "project rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.PROJECT_TABLESCAN_PUSHDOWN_RULES)
            .build(), "push project to table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.BATCH_EXEC_WINDOW_RULES)
            .build(), "window")
        .build())


    // optimize the logical plan
    programs.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_LOGICAL_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())

    // logical rewrite
    programs.addLast(
      LOGICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .add(FlinkBatchExecRuleSets.LOGICAL_REWRITE)
      .build())

    // optimize the physical plan
    programs.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkBatchExecRuleSets.BATCH_EXEC_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.BATCH_PHYSICAL))
        .build())

    // physical rewrite
    programs.addLast(
      PHYSICAL_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[BatchOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.BATCH_EXEC_POST_PHYSICAL_RULES)
            .build(), "physical rewrite")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.RUNTIME_FILTER_RULES)
            .build(), "runtime filter insert and push down")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkBatchExecRuleSets.RUNTIME_FILTER_REMOVE_RULES)
            .build(), "runtime filter remove useless")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkBatchExecRuleSets.SKEW_AGGREGATE_REWRITE_RULES)
          .build(), "skewed aggregate rewrite")
        .build())

    programs
  }
}
