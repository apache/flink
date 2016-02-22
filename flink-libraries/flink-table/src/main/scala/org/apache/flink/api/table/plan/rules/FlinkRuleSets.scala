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

package org.apache.flink.api.table.plan.rules

import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSets, RuleSet}
import org.apache.flink.api.table.plan.rules.logical._
import org.apache.flink.api.table.plan.rules.dataset._

object FlinkRuleSets {

  /**
    * RuleSet to optimize plans for batch / DataSet exeuction
    */
  val DATASET_OPT_RULES: RuleSet = RuleSets.ofList(

    // filter rules
    FilterJoinRule.FILTER_ON_JOIN,
    FilterJoinRule.JOIN,
    FilterMergeRule.INSTANCE,
    FilterAggregateTransposeRule.INSTANCE,

    // push and merge projection rules
    AggregateProjectMergeRule.INSTANCE,
    ProjectMergeRule.INSTANCE,
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE,
    ProjectJoinTransposeRule.INSTANCE,
    ProjectRemoveRule.INSTANCE,
    SortProjectTransposeRule.INSTANCE,
    ProjectSortTransposeRule.INSTANCE,

    // merge and push unions rules
    // TODO: Add a rule to enforce binary unions
    UnionEliminatorRule.INSTANCE,
    JoinUnionTransposeRule.LEFT_UNION,
    JoinUnionTransposeRule.RIGHT_UNION,
    // non-all Union to all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // aggregation rules
    AggregateRemoveRule.INSTANCE,
    AggregateJoinTransposeRule.EXTENDED,
    AggregateUnionAggregateRule.INSTANCE,
    // deactivate this rule temporarily
    // AggregateReduceFunctionsRule.INSTANCE,
    AggregateExpandDistinctAggregatesRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // join reordering rules
    JoinPushThroughJoinRule.LEFT,
    JoinPushThroughJoinRule.RIGHT,
    JoinAssociateRule.INSTANCE,
    JoinCommuteRule.INSTANCE,
    JoinCommuteRule.SWAP_OUTER,

    // simplify expressions rules
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE,

    // prune empty results rules
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE,

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    CalcMergeRule.INSTANCE,

    // translate to logical Flink nodes
    FlinkAggregateRule.INSTANCE,
    FlinkCalcRule.INSTANCE,
    FlinkJoinRule.INSTANCE,
    FlinkScanRule.INSTANCE,
    FlinkUnionRule.INSTANCE
  )

  val DATASET_TRANS_RULES: RuleSet = RuleSets.ofList(
  
    // translate to DataSet nodes
    DataSetAggregateRule.INSTANCE,
    DataSetCalcRule.INSTANCE,
    DataSetJoinRule.INSTANCE,
    DataSetScanRule.INSTANCE,
    DataSetUnionRule.INSTANCE
  )

}
