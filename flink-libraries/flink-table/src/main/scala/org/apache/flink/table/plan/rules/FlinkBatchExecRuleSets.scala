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
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter._

import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.{LogicalIntersect, LogicalMinus, LogicalUnion}
import org.apache.calcite.rel.rules.{ReduceExpressionsRule, _}
import org.apache.calcite.tools.{RuleSet, RuleSets}

import scala.collection.JavaConverters._

object FlinkBatchExecRuleSets {

  val SEMI_JOIN_RULES: RuleSet = RuleSets.ofList(
    FilterSimplifyExpressionsRule.EXTENDED,
    FlinkRewriteSubQueryRule.FILTER,
    FlinkSubQueryRemoveRule.FILTER,
    JoinConditionTypeCoerceRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE
  )

  /**
    * Convert sub-queries before query decorrelation.
    */
  val TABLE_SUBQUERY_RULES: RuleSet = RuleSets.ofList(
    SubQueryRemoveRule.FILTER,
    SubQueryRemoveRule.PROJECT,
    SubQueryRemoveRule.JOIN)


  /**
    * Expand plan by replacing references to tables into a proper plan sub trees. Those rules
    * can create new plan nodes.
    */
  val EXPAND_PLAN_RULES: RuleSet = RuleSets.ofList(
    LogicalCorrelateToTemporalTableJoinRule.INSTANCE,
    TableScanRule.INSTANCE)

  val POST_EXPAND_CLEAN_UP_RULES: RuleSet = RuleSets.ofList(
    EnumerableToLogicalTableScan.INSTANCE)

  val UNNEST_RULES: RuleSet = RuleSets.ofList(
    LogicalUnnestRule.INSTANCE
  )

  /**
    * Convert table references before query decorrelation.
    */
  val TABLE_REF_RULES: RuleSet = RuleSets.ofList(
    // rules to convert catalog table to normal table.
    CatalogTableRules.BATCH_TABLE_SCAN_RULE,
    TableScanRule.INSTANCE,
    EnumerableToLogicalTableScan.INSTANCE)

  private val PREDICATE_SIMPLIFY_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    FilterSimplifyExpressionsRule.INSTANCE,
    JoinConditionSimplifyExpressionsRule.INSTANCE,
    JoinConditionTypeCoerceRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE
  )

  private val REDUCE_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    // reduce expressions rules
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE
  )

  private val REWRITE_COALESCE_RULES: RuleSet = RuleSets.ofList(
    // rewrite coalesce to case when
    FlinkRewriteCoalesceRule.FILTER_INSTANCE,
    FlinkRewriteCoalesceRule.PROJECT_INSTANCE,
    FlinkRewriteCoalesceRule.JOIN_INSTANCE,
    FlinkRewriteCoalesceRule.CALC_INSTANCE
  )

  private val LIMIT_RULES: RuleSet = RuleSets.ofList(
    //push down localLimit
    PushLimitIntoTableSourceScanRule.INSTANCE)

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
        FilterMergeRule.INSTANCE)

  val FILTER_PREPARE_RULES: RuleSet =  RuleSets.ofList((
      FILTER_RULES.asScala
          // simplify predicate expressions in filters and joins
          ++ PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala
          // reduce expressions in filters and joins
          ++ REDUCE_EXPRESSION_RULES.asScala
      ).asJava)

  val FILTER_TABLESCAN_PUSHDOWN_RULES: RuleSet = RuleSets.ofList(
    // push a filter down into the table scan
    PushFilterIntoTableSourceScanRule.INSTANCE
  )

  val PRUNE_EMPTY_RULES: RuleSet = RuleSets.ofList(
    // prune empty results rules
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    FlinkPruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE
  )

  val PROJECT_RULES: RuleSet = RuleSets.ofList(
    // push a projection past a filter
    ProjectFilterTransposeRule.INSTANCE,
    // push a projection to the children of a join
    // push all expressions to handle the time indicator correctly
    new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
    // merge projections
    ProjectMergeRule.INSTANCE,
    // remove identity project
    ProjectRemoveRule.INSTANCE,
    // reorder sort and projection
    ProjectSortTransposeRule.INSTANCE,
    // push a projection to the children of a SemiJoin
    ProjectSemiJoinTransposeRule.INSTANCE,
    //removes constant keys from an Agg
    AggregateProjectPullUpConstantsRule.INSTANCE
  )

  val PROJECT_TABLESCAN_PUSHDOWN_RULES: RuleSet = RuleSets.ofList(
    // push a project down into the table scan
    PushProjectIntoTableSourceScanRule.INSTANCE
  )

  val SKEW_JOIN_REWRITE_RULES: RuleSet = RuleSets.ofList(
    SkewedJoinRule.INSTANCE
  )

  val SKEW_AGGREGATE_REWRITE_RULES: RuleSet = RuleSets.ofList(
    SplitCompleteHashAggRule.INSTANCE,
    SplitCompleteSortAggRule.INSTANCE
  )

  val JOIN_COND_EQUAL_TRANSFER_RULES: RuleSet = RuleSets.ofList((
      RuleSets.ofList(JoinCondEqualityTransferRule.INSTANCE).asScala ++
          PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
          FILTER_RULES.asScala
  ).asJava)

  private val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,

    // join rules
    JoinPushExpressionsRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    FlinkAggregateRemoveRule.INSTANCE,
    // push aggregate through join
    FlinkAggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    AggregateUnionAggregateRule.INSTANCE,
    // expand distinct aggregate to normal aggregate with groupby
    FlinkAggregateExpandDistinctAggregatesRule.INSTANCE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    AggregateReduceFunctionsRule.INSTANCE,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // reduce group by columns
    AggregateReduceGroupingRule.INSTANCE,
    // reduce useless aggCall
    PruneAggregateCallRule.PROJECT_ON_AGGREGATE,
    PruneAggregateCallRule.CALC_ON_AGGREGATE,

    // expand grouping sets
    DecomposeGroupingSetsRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // rank rules
    FlinkLogicalRankRule.CONSTANT_RANK,
    CalcRankMergeRule.INSTANCE,

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    FlinkCalcMergeRule.INSTANCE,

    // scan optimization
    PushProjectIntoTableSourceScanRule.INSTANCE,
    PushFilterIntoTableSourceScanRule.INSTANCE,

    // semi-join transpose rule
    FlinkSemiJoinJoinTransposeRule.INSTANCE,
    FlinkSemiJoinProjectTransposeRule.INSTANCE,
    SemiJoinFilterTransposeRule.INSTANCE,

    // set operators
    ReplaceIntersectWithSemiJoinRule.INSTANCE,
    ReplaceExceptWithAntiJoinRule.INSTANCE
  )

  val LOGICAL_REWRITE: RuleSet = RuleSets.ofList(
    // transpose calc past snapshot
    CalcSnapshotTransposeRule.INSTANCE,
    // merge calc after calc transpose
    CalcMergeRule.INSTANCE)

  private val BATCH_EXEC_LOGICAL_CONVERTERS: RuleSet = RuleSets.ofList(
    // translate to flink logical rel nodes
    FlinkLogicalAggregate.BATCH_CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalOverWindow.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSemiJoin.CONVERTER,
    FlinkLogicalSort.BATCH_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalNativeTableScan.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalMatch.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalRank.CONVERTER,
    FlinkLogicalSink.CONVERTER
  )

  /**
    * RuleSet to do logical optimize for batch exec execution
    */
  val BATCH_EXEC_LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList((
      LIMIT_RULES.asScala ++
          FILTER_RULES.asScala ++
          PROJECT_RULES.asScala ++
          PRUNE_EMPTY_RULES.asScala ++
          LOGICAL_OPT_RULES.asScala ++
          BATCH_EXEC_LOGICAL_CONVERTERS.asScala
      ).asJava)

  /**
    * RuleSet to normalize plans for batch exec execution
    */
  val BATCH_EXEC_DEFAULT_REWRITE_RULES: RuleSet = RuleSets.ofList((
      PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
          REWRITE_COALESCE_RULES.asScala ++
          REDUCE_EXPRESSION_RULES.asScala ++
          List(
            // Transform window to LogicalWindowAggregate
            BatchExecLogicalWindowAggregateRule.INSTANCE,
            WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
            WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE,
            //ensure union set operator have the same row type
            new CoerceInputsRule(classOf[LogicalUnion], false),
            //ensure intersect set operator have the same row type
            new CoerceInputsRule(classOf[LogicalIntersect], false),
            //ensure except set operator have the same row type
            new CoerceInputsRule(classOf[LogicalMinus], false),
            MergeMultiEqualsToInRule.INSTANCE,
            MergeMultiNotEqualsToNotInRule.INSTANCE,
            // optimize limit 0
            FlinkLimitRemoveRule.INSTANCE,

            // unnest rule
            LogicalUnnestRule.INSTANCE
          )).asJava)

  val BATCH_EXEC_JOIN_PREDICATE_REWRITE_RULES: RuleSet = RuleSets.ofList(
    JoinDependentFilterPushdownRule.INSTANCE,
    JoinDeriveNullFilterRule.INSTANCE
  )

  val BATCH_EXEC_WINDOW_RULES: RuleSet = RuleSets.ofList(
    // slices a project into sections which contain window agg functions and sections which do not.
    ProjectToWindowRule.PROJECT,
    //adjust the sequence of window's groups.
    ExchangeWindowGroupRule.INSTANCE,
    // Transform window to LogicalWindowAggregate
    WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
    WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE
  )

  val BATCH_EXEC_JOIN_REORDER: RuleSet = RuleSets.ofList(
    // reorder join
    FilterSimplifyExpressionsRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE,
    FlinkFilterJoinRule.FILTER_ON_JOIN,
    FilterAggregateTransposeRule.INSTANCE,
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
    JoinToMultiJoinRule.INSTANCE,
    ProjectMultiJoinMergeRule.INSTANCE,
    FilterMultiJoinMergeRule.INSTANCE,
    // we put rewrite-self-join together with join reorder cause
    // there is no good way to recover the MultiJoin back to
    // what it originally is if we match rewrite-self-join failed,
    // will make a new program when we can do this.
    RewriteSelfJoinRule.COMPLEX,
    RewriteSelfJoinRule.SIMPLE,
    RewriteMultiJoinConditionRule.INSTANCE,
    LoptOptimizeJoinRule.INSTANCE
  )

  /**
    * RuleSet to optimize plans for batchExec / DataStream execution
    */
  val BATCH_EXEC_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.BATCH_INSTANCE,

    BatchExecScanTableRule.INSTANCE,
    BatchExecIntermediateTableScanRule.INSTANCE,
    BatchExecScanTableSourceRule.INSTANCE,
    BatchExecCalcRule.INSTANCE,
    BatchExecCorrelateRule.INSTANCE,
    BatchExecValuesRule.INSTANCE,
    //sort
    BatchExecSortLimitRule.INSTANCE,
    BatchExecSortRule.INSTANCE,
    BatchExecLimitRule.INSTANCE,
    //join
    BatchExecSingleRowJoinRule.INSTANCE,
    BatchExecSingleRowJoinRule.SEMI_JOIN,
    BatchExecHashJoinRule.INSTANCE,
    BatchExecHashJoinRule.SEMI_JOIN,
    BatchExecSortMergeJoinRule.INSTANCE,
    BatchExecSortMergeJoinRule.SEMI_JOIN,
    BatchExecNestedLoopJoinRule.INSTANCE,
    BatchExecNestedLoopJoinRule.SEMI_JOIN,
    // group agg
    BatchExecSortAggRule.INSTANCE,
    BatchExecHashAggRule.INSTANCE,
    RemoveRedundantLocalSortAggRule.WITHOUT_SORT,
    RemoveRedundantLocalSortAggRule.WITH_SORT,
    RemoveRedundantLocalHashAggRule.INSTANCE,
    // over window agg
    BatchExecOverWindowAggRule.INSTANCE,
    // group window agg
    BatchExecWindowAggregateRule.INSTANCE,
    //union
    BatchExecUnionRule.INSTANCE,
    //expand
    BatchExecExpandRule.INSTANCE,

    BatchExecTemporalTableJoinRule.SNAPSHOT_ON_TABLESCAN,
    BatchExecTemporalTableJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    // rank
    BatchExecRankRule.INSTANCE,
    RemoveRedundantLocalRankRule.INSTANCE,
    // sink
    BatchExecSinkRule.INSTANCE
  )

  val BATCH_EXEC_POST_PHYSICAL_RULES: RuleSet = RuleSets.ofList(
    // push project into correlate
    BatchExecPushProjectIntoCorrelateRule.INSTANCE
  )

  val RUNTIME_FILTER_RULES: RuleSet = RuleSets.ofList(
    InsertRuntimeFilterRule.INSTANCE,
    RuntimeFilterAggTransposeRule.INSTANCE,
    RuntimeFilterExchangeTransposeRule.INSTANCE,
    RfBuilderExchangeTransposeRule.INSTANCE,
    RfBuilderJoinTransposeRule.INSTANCE,
    RuntimeFilterBuilderMerger.INSTANCE,
    FlinkCalcMergeRule.INSTANCE
  )

  val RUNTIME_FILTER_REMOVE_RULES: RuleSet = RuleSets.ofList(
    UselessRuntimeFilterRemoveRule.INSTANCE,
    UselessRfBuilderRemoveRule.INSTANCE,
    FlinkCalcRemoveRule.INSTANCE
  )
}
