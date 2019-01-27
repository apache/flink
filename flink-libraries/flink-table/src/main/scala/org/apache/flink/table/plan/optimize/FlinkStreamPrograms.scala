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
import org.apache.flink.table.plan.rules.FlinkStreamExecRuleSets

import org.apache.calcite.plan.hep.HepMatchOrder

/**
  * Defines a sequence of programs to optimize for stream table plan.
  */
object FlinkStreamPrograms {

  val SUBQUERY_REWRITE = "subquery_rewrite"
  val CORRELATE_REWRITE = "correlate_rewrite"
  val DECORRELATE = "decorrelate"
  val TIME_INDICATOR = "time_indicator"
  val DEFAULT_REWRITE = "default_rewrite"
  val PREDICATE_PUSHDOWN = "predicate_pushdown"
  val JOIN_REORDER = "join_reorder"
  val WINDOW = "window"
  val LOGICAL = "logical"
  val LOGICAL_REWRITE = "logical_rewrite"
  val PHYSICAL = "physical"
  val PHYSICAL_REWRITE = "physical_rewrite"

  def buildPrograms(config: Configuration): FlinkChainedPrograms[StreamOptimizeContext] = {
    val programs = new FlinkChainedPrograms[StreamOptimizeContext]()

    // rewrite sub-queries to joins
    programs.addLast(
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        // rewrite RelTable before rewriting sub-queries
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references before rewriting sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.SEMI_JOIN_RULES)
          .build(), "rewrite sub-queries to semi-join")
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.TABLE_SUBQUERY_RULES)
          .build(), "sub-queries remove")
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.TABLE_REF_RULES)
          .build(), "convert table references after sub-queries removed")
        .build())

    // rewrite special temporal join plan
    programs.addLast(
      CORRELATE_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.EXPAND_PLAN_RULES)
            .build(), "convert correlate to temporal table join")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.POST_EXPAND_CLEAN_UP_RULES)
            .build(), "convert enumerable table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.UNNEST_RULES)
            .build(), "convert unnest into table function scan")
        .build())

    // query decorrelation
    programs.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(new FlinkDecorrelateProgram, "decorrelate")
        .addProgram(new FlinkCorrelateVariablesValidationProgram, "correlate variables validation")
        .build()
    )

    // convert time indicators
    programs.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram)

    // default rewrite, includes: predicate simplification, expression reduction, window
    // properties rewrite, etc.
    programs.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_DEFAULT_REWRITE_RULES)
        .build())

    // rule based optimization: push down predicate(s) in where clause, so it only needs to read
    // the required data
    programs.addLast(
      PREDICATE_PUSHDOWN,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.FILTER_PREPARE_RULES)
            .build(), "filter rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.FILTER_TABLESCAN_PUSHDOWN_RULES)
            .build(), "push filter to table scan")
        .build())

    // join reorder
    if (config.getBoolean(TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED)) {
      programs.addLast(
        JOIN_REORDER,
        FlinkHepRuleSetProgramBuilder.newBuilder
          .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
          .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
          .add(FlinkStreamExecRuleSets.STREAM_EXEC_JOIN_REORDER)
          .build())
    }

    // window
    programs.addLast(
      WINDOW,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.PROJECT_RULES)
            .build(), "project rules")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.PROJECT_TABLESCAN_PUSHDOWN_RULES)
            .build(), "push project to table scan")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.STREAM_EXEC_WINDOW_RULES)
            .build(), "window")
        .build())

    // optimize the logical Flink plan
    programs.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_LOGICAL_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.LOGICAL))
        .build())

    // logical rewrite
    programs.addLast(
      LOGICAL_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamExecRuleSets.LOGICAL_REWRITE)
        .build())

    // optimize the physical plan
    programs.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamExecRuleSets.STREAM_EXEC_OPT_RULES)
        .setTargetTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build())

    // physical rewrite
    programs.addLast(
      PHYSICAL_REWRITE,
      FlinkGroupProgramBuilder.newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkDecorateProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.STREAM_EXEC_DECORATE_RULES)
            .build(), "decorate")
        .addProgram(new FlinkMiniBatchAnalyseProgram, "micro batch")
        .addProgram(
          FlinkDecorateProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamExecRuleSets.PHYSICAL_REWRITE)
            .build(), "physical rewrite")
        .build())

    programs
  }
}
