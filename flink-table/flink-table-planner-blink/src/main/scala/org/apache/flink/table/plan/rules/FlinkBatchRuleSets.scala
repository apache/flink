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

package org.apache.flink.table.plan.rules

import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.rules.logical._
import org.apache.flink.table.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.plan.rules.physical.batch._

import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.{LogicalIntersect, LogicalMinus, LogicalUnion}
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}

import scala.collection.JavaConverters._

object FlinkBatchRuleSets {

  val SEMI_JOIN_RULES: RuleSet = RuleSets.ofList(
    SimplifyFilterConditionRule.EXTENDED,
    FlinkSubQueryRemoveRule.FILTER,
    FlinkJoinPushExpressionsRule.INSTANCE
  )

  /**
    * Convert sub-queries before query decorrelation.
    */
  val TABLE_SUBQUERY_RULES: RuleSet = RuleSets.ofList(
    SubQueryRemoveRule.FILTER,
    SubQueryRemoveRule.PROJECT,
    SubQueryRemoveRule.JOIN
  )

  /**
    * Expand plan by replacing references to tables into a proper plan sub trees. Those rules
    * can create new plan nodes.
    */
  val EXPAND_PLAN_RULES: RuleSet = RuleSets.ofList(
    LogicalCorrelateToTemporalTableJoinRule.INSTANCE,
    TableScanRule.INSTANCE)

  val POST_EXPAND_CLEAN_UP_RULES: RuleSet = RuleSets.ofList(
    EnumerableToLogicalTableScan.INSTANCE)

  /**
    * Convert table references before query decorrelation.
    */
  val TABLE_REF_RULES: RuleSet = RuleSets.ofList(
    TableScanRule.INSTANCE,
    EnumerableToLogicalTableScan.INSTANCE
  )

  /**
    * RuleSet to reduce expressions
    */
  private val REDUCE_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE
  )

  /**
    * RuleSet to rewrite coalesce to case when
    */
  private val REWRITE_COALESCE_RULES: RuleSet = RuleSets.ofList(
    // rewrite coalesce to case when
    RewriteCoalesceRule.FILTER_INSTANCE,
    RewriteCoalesceRule.PROJECT_INSTANCE,
    RewriteCoalesceRule.JOIN_INSTANCE,
    RewriteCoalesceRule.CALC_INSTANCE
  )

  /**
    * RuleSet to normalize plans for batch
    */
  val DEFAULT_REWRITE_RULES: RuleSet = RuleSets.ofList((
    REWRITE_COALESCE_RULES.asScala ++
      REDUCE_EXPRESSION_RULES.asScala ++
      List(
        // Transform window to LogicalWindowAggregate
        BatchLogicalWindowAggregateRule.INSTANCE,
        WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
        WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE,
        //ensure union set operator have the same row type
        new CoerceInputsRule(classOf[LogicalUnion], false),
        //ensure intersect set operator have the same row type
        new CoerceInputsRule(classOf[LogicalIntersect], false),
        //ensure except set operator have the same row type
        new CoerceInputsRule(classOf[LogicalMinus], false),
        ConvertToNotInOrInRule.INSTANCE
      )).asJava)

  /**
    * RuleSet about filter
    */
  private val FILTER_RULES: RuleSet = RuleSets.ofList(
    // push a filter into a join
    FlinkFilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FlinkFilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,
    // push a filter past a project
    FilterProjectTransposeRule.INSTANCE,
    FilterSetOpTransposeRule.INSTANCE,
    FilterMergeRule.INSTANCE
  )

  /**
    * Ruleset to simplify expressions
    */
  private val PREDICATE_SIMPLIFY_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    // TODO: add filter simply and join condition simplify rules
    JoinPushExpressionsRule.INSTANCE
  )

  /**
    * RuleSet to do predicate pushdown
    */
  val FILTER_PREPARE_RULES: RuleSet = RuleSets.ofList((
    FILTER_RULES.asScala
      // simplify expressions
      ++ PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala
      // reduce expressions in filters and joins
      ++ REDUCE_EXPRESSION_RULES.asScala
    ).asJava
  )

  /**
    * RuleSet to prune empty results rules
    */
  val PRUNE_EMPTY_RULES: RuleSet = RuleSets.ofList(
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE
  )

  /**
    * RuleSet about project
    */
  val PROJECT_RULES: RuleSet = RuleSets.ofList(
    // push a projection past a filter
    ProjectFilterTransposeRule.INSTANCE,
    // push a projection to the children of a join
    // push all expressions to handle the time indicator correctly
    new FlinkProjectJoinTransposeRule(
      PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
    // push a projection to the children of a semi/anti Join
    ProjectSemiAntiJoinTransposeRule.INSTANCE,
    // merge projections
    ProjectMergeRule.INSTANCE,
    // remove identity project
    ProjectRemoveRule.INSTANCE,
    // reorder sort and projection
    ProjectSortTransposeRule.INSTANCE,
    //removes constant keys from an Agg
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // push project through a Union
    ProjectSetOpTransposeRule.INSTANCE
  )

  val WINDOW_RULES: RuleSet = RuleSets.ofList(
    // slices a project into sections which contain window agg functions and sections which do not.
    ProjectToWindowRule.PROJECT,
    // TODO add ExchangeWindowGroupRule
    // Transform window to LogicalWindowAggregate
    WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
    WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE
  )

  /**
    * RuleSet to do logical optimize.
    * This RuleSet is a sub-set of [[LOGICAL_OPT_RULES]].
    */
  private val LOGICAL_RULES: RuleSet = RuleSets.ofList(
    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,

    // join rules
    FlinkJoinPushExpressionsRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    AggregateRemoveRule.INSTANCE,
    // push aggregate through join
    FlinkAggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    AggregateUnionAggregateRule.INSTANCE,
    // expand distinct aggregate to normal aggregate with groupby
    FlinkAggregateExpandDistinctAggregatesRule.INSTANCE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    AggregateReduceFunctionsRule.INSTANCE,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // expand grouping sets
    DecomposeGroupingSetsRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // rank rules
    FlinkLogicalRankRule.CONSTANT_RANGE_INSTANCE,

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    FlinkCalcMergeRule.INSTANCE,

    // semi/anti join transpose rule
    FlinkSemiAntiJoinJoinTransposeRule.INSTANCE,
    FlinkSemiAntiJoinProjectTransposeRule.INSTANCE,
    FlinkSemiAntiJoinFilterTransposeRule.INSTANCE
  )

  /**
    * RuleSet to translate calcite nodes to flink nodes
    */
  private val LOGICAL_CONVERTERS: RuleSet = RuleSets.ofList(
    FlinkLogicalAggregate.BATCH_CONVERTER,
    FlinkLogicalOverWindow.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSort.BATCH_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalDataStreamTableScan.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalRank.CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalSink.CONVERTER
  )

  /**
    * RuleSet to do logical optimize for batch
    */
  val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList((
    FILTER_RULES.asScala ++
      PROJECT_RULES.asScala ++
      PRUNE_EMPTY_RULES.asScala ++
      LOGICAL_RULES.asScala ++
      LOGICAL_CONVERTERS.asScala
    ).asJava)

  /**
    * RuleSet to do rewrite on FlinkLogicalRel for batch
    */
  val LOGICAL_REWRITE: RuleSet = RuleSets.ofList(
    // transpose calc past snapshot
    CalcSnapshotTransposeRule.INSTANCE,
    // merge calc after calc transpose
    FlinkCalcMergeRule.INSTANCE
  )

  /**
    * RuleSet to do physical optimize for batch
    */
  val PHYSICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.BATCH_INSTANCE,
    BatchExecBoundedStreamScanRule.INSTANCE,
    BatchExecScanTableSourceRule.INSTANCE,
    BatchExecIntermediateTableScanRule.INSTANCE,
    BatchExecValuesRule.INSTANCE,
    BatchExecCalcRule.INSTANCE,
    BatchExecUnionRule.INSTANCE,
    BatchExecSortRule.INSTANCE,
    BatchExecLimitRule.INSTANCE,
    BatchExecSortLimitRule.INSTANCE,
    BatchExecRankRule.INSTANCE,
    BatchExecExpandRule.INSTANCE,
    BatchExecHashAggRule.INSTANCE,
    BatchExecSortAggRule.INSTANCE,
    BatchExecHashJoinRule.INSTANCE,
    BatchExecSortMergeJoinRule.INSTANCE,
    BatchExecNestedLoopJoinRule.INSTANCE,
    BatchExecSingleRowJoinRule.INSTANCE,
    BatchExecCorrelateRule.INSTANCE,
    BatchExecOverWindowAggRule.INSTANCE,
    BatchExecWindowAggregateRule.INSTANCE,
    BatchExecLookupJoinRule.SNAPSHOT_ON_TABLESCAN,
    BatchExecLookupJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    BatchExecSinkRule.INSTANCE
  )
}
