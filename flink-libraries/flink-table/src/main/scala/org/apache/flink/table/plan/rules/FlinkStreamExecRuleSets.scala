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

import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.{LogicalIntersect, LogicalMinus, LogicalUnion}
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.rules.logical._
import org.apache.flink.table.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.plan.rules.physical.stream._

import scala.collection.JavaConverters._

object FlinkStreamExecRuleSets {

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
    LogicalCorrelateToTemporalTableFunctionJoinRule.INSTANCE,
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
    CatalogTableRules.STREAM_TABLE_SCAN_RULE,
    TableScanRule.INSTANCE,
    EnumerableToLogicalTableScan.INSTANCE)

  private val PREDICATE_SIMPLIFY_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    FilterSimplifyExpressionsRule.INSTANCE,
    JoinConditionSimplifyExpressionsRule.INSTANCE,
    JoinConditionTypeCoerceRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE
  )

  private val REWRITE_COALESCE_RULES: RuleSet = RuleSets.ofList(
    // rewrite coalesce to case when
    FlinkRewriteCoalesceRule.FILTER_INSTANCE,
    FlinkRewriteCoalesceRule.PROJECT_INSTANCE,
    FlinkRewriteCoalesceRule.JOIN_INSTANCE,
    FlinkRewriteCoalesceRule.CALC_INSTANCE
  )

  private val REDUCE_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    // reduce expressions rules
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE
  )

  private val FILTER_RULES: RuleSet = RuleSets.ofList(
    // push a filter into a join
    FlinkFilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FlinkFilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,
    // push a filter past a project
    FilterProjectTransposeRule.INSTANCE,
    // push a filter past a setop
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
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // push project through a Union
    ProjectSetOpTransposeRule.INSTANCE
  )

  val PROJECT_TABLESCAN_PUSHDOWN_RULES: RuleSet = RuleSets.ofList(
    // push a project down into the table scan
    PushProjectIntoTableSourceScanRule.INSTANCE
  )

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
    FlinkAggregateJoinTransposeRule.LEFT_RIGHT_OUTER_JOIN_EXTENDED,
    // using variants of aggregate union rule
    AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
    AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    AggregateReduceFunctionsRule.INSTANCE,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // reduce useless aggCall
    PruneAggregateCallRule.PROJECT_ON_AGGREGATE,
    PruneAggregateCallRule.CALC_ON_AGGREGATE,

    // expand grouping sets
    DecomposeGroupingSetsRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

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

  private val STREAM_EXEC_LOGICAL_CONVERTERS: RuleSet = RuleSets.ofList(
    // translate to flink logical rel nodes
    FlinkLogicalAggregate.STREAM_CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalOverWindow.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSemiJoin.CONVERTER,
    FlinkLogicalSort.STREAM_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalNativeTableScan.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalMatch.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalWatermarkAssigner.CONVERTER,
    FlinkLogicalLastRow.CONVERTER,
    FlinkLogicalSink.CONVERTER
  )

  /**
    * RuleSet to do logical optimize for stream exec execution
    */
  val STREAM_EXEC_LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList((
    FILTER_RULES.asScala ++
    PROJECT_RULES.asScala ++
    PRUNE_EMPTY_RULES.asScala ++
    LOGICAL_OPT_RULES.asScala ++
    STREAM_EXEC_LOGICAL_CONVERTERS.asScala
  ).asJava)

  /**
    * RuleSet to normalize plans for stream exec execution
    */
  val STREAM_EXEC_DEFAULT_REWRITE_RULES: RuleSet = RuleSets.ofList((
      PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
        REWRITE_COALESCE_RULES.asScala ++
        REDUCE_EXPRESSION_RULES.asScala ++
        List(
          StreamExecLogicalWindowAggregateRule.INSTANCE,
          // slices a project into sections which contain window agg functions
          // and sections which do not.
          ProjectToWindowRule.PROJECT,
          WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
          WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE,
          //ensure union set operator have the same row type
          new CoerceInputsRule(classOf[LogicalUnion], false),
          //ensure intersect set operator have the same row type
          new CoerceInputsRule(classOf[LogicalIntersect], false),
          //ensure except set operator have the same row type
          new CoerceInputsRule(classOf[LogicalMinus], false),
          // optimize limit 0
          FlinkLimitRemoveRule.INSTANCE,

          MergeMultiEqualsToInRule.INSTANCE,
          MergeMultiNotEqualsToNotInRule.INSTANCE
        )
      ).asJava)

  val STREAM_EXEC_WINDOW_RULES: RuleSet = RuleSets.ofList(
    // Transform window to LogicalWindowAggregate
    WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
    WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE
  )

  val STREAM_EXEC_JOIN_REORDER: RuleSet = RuleSets.ofList(
    //reorder join
    JoinPushExpressionsRule.INSTANCE,
    FlinkFilterJoinRule.FILTER_ON_JOIN,
    FilterAggregateTransposeRule.INSTANCE,
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
    JoinToMultiJoinRule.INSTANCE,
    ProjectMultiJoinMergeRule.INSTANCE,
    FilterMultiJoinMergeRule.INSTANCE,
    RewriteMultiJoinConditionRule.INSTANCE,
    LoptOptimizeJoinRule.INSTANCE
  )

  val LOGICAL_REWRITE: RuleSet = RuleSets.ofList(
    // transform over window to topn node
    FlinkLogicalRankRule.INSTANCE,
    // transpose calc past lastrow to reduce state size
    CalcLastRowTransposeRule.INSTANCE,
    // split distinct aggregate to reduce data skew
    SplitAggregateRule.INSTANCE,
    // transpose calc past snapshot
    CalcSnapshotTransposeRule.INSTANCE,
    // merge calc after calc transpose
    CalcMergeRule.INSTANCE)

  /**
    * RuleSet to optimize plans for streamExec / DataStream execution
    */
  val STREAM_EXEC_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.STREAM_INSTANCE,
    // translate to StreamExec nodes
    StreamExecSortRule.INSTANCE,
    StreamExecGroupAggregateRule.INSTANCE,
    StreamExecOverAggregateRule.INSTANCE,
    StreamExecGroupWindowAggregateRule.INSTANCE,
    StreamExecCalcRule.INSTANCE,
    StreamExecScanRule.INSTANCE,
    StreamExecIntermediateTableScanRule.INSTANCE,
    StreamExecUnionRule.INSTANCE,
    StreamExecJoinRule.INSTANCE,
    StreamExecSemiJoinRule.INSTANCE,
    StreamExecTemporalTableFunctionJoinRule.INSTANCE,
    StreamExecTemporalTableJoinRule.SNAPSHOT_ON_TABLESCAN,
    StreamExecTemporalTableJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    StreamExecValuesRule.INSTANCE,
    StreamExecCorrelateRule.INSTANCE,
    StreamExecWindowJoinRule.INSTANCE,
    StreamExecTableSourceScanRule.INSTANCE,
    StreamExecMatchRule.INSTANCE,
    StreamExecRankRules.SORT_INSTANCE,
    StreamExecRankRules.RANK_INSTANCE,
    StreamExecWatermarkAssignerRule.INSTANCE,
    StreamExecExpandRule.INSTANCE,
    StreamExecLastRowRule.INSTANCE,
    // sink
    StreamExecSinkRule.INSTANCE
  )

  /**
    * RuleSet to decorate plans for stream exec execution
    */
  val STREAM_EXEC_DECORATE_RULES: RuleSet = RuleSets.ofList(
    // retraction rules
    StreamExecRetractionRules.DEFAULT_RETRACTION_INSTANCE,
    StreamExecRetractionRules.UPDATES_AS_RETRACTION_INSTANCE,
    StreamExecRetractionRules.ACCMODE_INSTANCE)

  /**
    * RuleSet to optimize plans after stream exec execution.
    */
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    //optimize agg rule
    TwoStageOptimizedAggregateRule.INSTANCE,
    // incremental agg rule
    IncrementalAggregateRule.INSTANCE,
    StreamExecUnionTransposeRule.LOCAL_GROUP_AGG_INSTANCE,
    StreamExecUnionTransposeRule.EXPAND_INSTANCE,
    StreamExecUnionTransposeRule.CALC_INSTANCE,
    CalcMergeRule.INSTANCE,
    // project correlate's left input
    StreamExecPushProjectIntoCorrelateRule.INSTANCE
  )
}
