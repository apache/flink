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

package org.apache.flink.table.planner.plan.rules

import org.apache.flink.table.planner.plan.nodes.logical._
import org.apache.flink.table.planner.plan.rules.logical._
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.planner.plan.rules.physical.stream._

import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.{LogicalIntersect, LogicalMinus, LogicalUnion}
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}

import scala.collection.JavaConverters._

object FlinkStreamRuleSets {

  val SEMI_JOIN_RULES: RuleSet = RuleSets.ofList(
    SimplifyFilterConditionRule.EXTENDED,
    FlinkRewriteSubQueryRule.FILTER,
    FlinkSubQueryRemoveRule.FILTER,
    JoinConditionTypeCoerceRule.INSTANCE,
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
    LogicalCorrelateToJoinFromTemporalTableRule.WITH_FILTER,
    LogicalCorrelateToJoinFromTemporalTableRule.WITHOUT_FILTER,
    LogicalCorrelateToJoinFromTemporalTableFunctionRule.INSTANCE,
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
    * RuleSet to simplify predicate expressions in filters and joins
    */
  private val PREDICATE_SIMPLIFY_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    SimplifyFilterConditionRule.INSTANCE,
    SimplifyJoinConditionRule.INSTANCE,
    JoinConditionTypeCoerceRule.INSTANCE,
    JoinPushExpressionsRule.INSTANCE
  )

  /**
    * RuleSet to normalize plans for stream
    */
  val DEFAULT_REWRITE_RULES: RuleSet = RuleSets.ofList((
    PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
      REWRITE_COALESCE_RULES.asScala ++
      REDUCE_EXPRESSION_RULES.asScala ++
      List(
        StreamLogicalWindowAggregateRule.INSTANCE,
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
        ConvertToNotInOrInRule.INSTANCE,
        // optimize limit 0
        FlinkLimit0RemoveRule.INSTANCE,
        // unnest rule
        LogicalUnnestRule.INSTANCE
      )
    ).asJava)

  /**
    * RuleSet about filter
    */
  private val FILTER_RULES: RuleSet = RuleSets.ofList(
    // push a filter into a join
    FilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,
    // push a filter past a project
    FilterProjectTransposeRule.INSTANCE,
    // push a filter past a setop
    FilterSetOpTransposeRule.INSTANCE,
    FilterMergeRule.INSTANCE
  )

  /**
    * RuleSet to do predicate pushdown
    */
  val FILTER_PREPARE_RULES: RuleSet = RuleSets.ofList((
    FILTER_RULES.asScala
      // simplify predicate expressions in filters and joins
      ++ PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala
      // reduce expressions in filters and joins
      ++ REDUCE_EXPRESSION_RULES.asScala
    ).asJava)

  /**
    * RuleSet to do push predicate/partition into table scan
    */
  val FILTER_TABLESCAN_PUSHDOWN_RULES: RuleSet = RuleSets.ofList(
    // push a filter down into the table scan
    PushFilterIntoTableSourceScanRule.INSTANCE,
    // push partition into the table scan
    PushPartitionIntoTableSourceScanRule.INSTANCE
  )

  /**
    * RuleSet to prune empty results rules
    */
  val PRUNE_EMPTY_RULES: RuleSet = RuleSets.ofList(
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    FlinkPruneEmptyRules.JOIN_RIGHT_INSTANCE,
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
    // push a projection to the children of a non semi/anti join
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

  val JOIN_REORDER_PREPARE_RULES: RuleSet = RuleSets.ofList(
    // merge project to MultiJoin
    ProjectMultiJoinMergeRule.INSTANCE,
    // merge filter to MultiJoin
    FilterMultiJoinMergeRule.INSTANCE,
    // merge join to MultiJoin
    JoinToMultiJoinRule.INSTANCE
  )

  val JOIN_REORDER_RULES: RuleSet = RuleSets.ofList(
    // equi-join predicates transfer
    RewriteMultiJoinConditionRule.INSTANCE,
    // join reorder
    LoptOptimizeJoinRule.INSTANCE
  )

  /**
    * RuleSet to do logical optimize.
    * This RuleSet is a sub-set of [[LOGICAL_OPT_RULES]].
    */
  private val LOGICAL_RULES: RuleSet = RuleSets.ofList(
    // scan optimization
    PushProjectIntoTableSourceScanRule.INSTANCE,
    PushFilterIntoTableSourceScanRule.INSTANCE,

    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,
    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

    // join rules
    FlinkJoinPushExpressionsRule.INSTANCE,
    SimplifyJoinConditionRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    FlinkAggregateRemoveRule.INSTANCE,
    // push aggregate through join
    FlinkAggregateJoinTransposeRule.EXTENDED,
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

    // calc rules
    FilterCalcMergeRule.INSTANCE,
    ProjectCalcMergeRule.INSTANCE,
    FilterToCalcRule.INSTANCE,
    ProjectToCalcRule.INSTANCE,
    FlinkCalcMergeRule.INSTANCE,

    // semi/anti join transpose rule
    FlinkSemiAntiJoinJoinTransposeRule.INSTANCE,
    FlinkSemiAntiJoinProjectTransposeRule.INSTANCE,
    FlinkSemiAntiJoinFilterTransposeRule.INSTANCE,

    // set operators
    ReplaceIntersectWithSemiJoinRule.INSTANCE,
    RewriteIntersectAllRule.INSTANCE,
    ReplaceMinusWithAntiJoinRule.INSTANCE,
    RewriteMinusAllRule.INSTANCE
  )

  /**
    * RuleSet to translate calcite nodes to flink nodes
    */
  private val LOGICAL_CONVERTERS: RuleSet = RuleSets.ofList(
    // translate to flink logical rel nodes
    FlinkLogicalAggregate.STREAM_CONVERTER,
    FlinkLogicalTableAggregate.CONVERTER,
    FlinkLogicalOverAggregate.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSort.STREAM_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalDataStreamTableScan.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalWatermarkAssigner.CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalWindowTableAggregate.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalMatch.CONVERTER,
    FlinkLogicalSink.CONVERTER
  )

  /**
    * RuleSet to do logical optimize for stream
    */
  val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList((
    FILTER_RULES.asScala ++
      PROJECT_RULES.asScala ++
      PRUNE_EMPTY_RULES.asScala ++
      LOGICAL_RULES.asScala ++
      LOGICAL_CONVERTERS.asScala
    ).asJava)

  /**
    * RuleSet to do rewrite on FlinkLogicalRel for Stream
    */
  val LOGICAL_REWRITE: RuleSet = RuleSets.ofList(
    // transform over window to topn node
    FlinkLogicalRankRule.INSTANCE,
    // transpose calc past rank to reduce rank input fields
    CalcRankTransposeRule.INSTANCE,
    // remove output of rank number when it is a constant
    RankNumberColumnRemoveRule.INSTANCE,
    // split distinct aggregate to reduce data skew
    SplitAggregateRule.INSTANCE,
    // transpose calc past snapshot
    CalcSnapshotTransposeRule.INSTANCE,
    // Rule that splits python ScalarFunctions from join conditions
    SplitPythonConditionFromJoinRule.INSTANCE,
    // Rule that splits python ScalarFunctions from
    // java/scala ScalarFunctions in correlate conditions
    SplitPythonConditionFromCorrelateRule.INSTANCE,
    // merge calc after calc transpose
    FlinkCalcMergeRule.INSTANCE,
    // Rule that splits python ScalarFunctions from java/scala ScalarFunctions.
    PythonCalcSplitRule.SPLIT_CONDITION,
    PythonCalcSplitRule.SPLIT_PROJECT,
    PythonCalcSplitRule.PUSH_CONDITION,
    PythonCalcSplitRule.REWRITE_PROJECT
  )

  /**
    * RuleSet to do physical optimize for stream
    */
  val PHYSICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.STREAM_INSTANCE,
    // source
    StreamExecDataStreamScanRule.INSTANCE,
    StreamExecTableSourceScanRule.INSTANCE,
    StreamExecIntermediateTableScanRule.INSTANCE,
    StreamExecWatermarkAssignerRule.INSTANCE,
    StreamExecValuesRule.INSTANCE,
    // calc
    StreamExecCalcRule.INSTANCE,
    StreamExecPythonCalcRule.INSTANCE,
    // union
    StreamExecUnionRule.INSTANCE,
    // sort
    StreamExecSortRule.INSTANCE,
    StreamExecLimitRule.INSTANCE,
    StreamExecSortLimitRule.INSTANCE,
    StreamExecTemporalSortRule.INSTANCE,
    // rank
    StreamExecRankRule.INSTANCE,
    StreamExecDeduplicateRule.RANK_INSTANCE,
    // expand
    StreamExecExpandRule.INSTANCE,
    // group agg
    StreamExecGroupAggregateRule.INSTANCE,
    StreamExecGroupTableAggregateRule.INSTANCE,
    // over agg
    StreamExecOverAggregateRule.INSTANCE,
    // window agg
    StreamExecGroupWindowAggregateRule.INSTANCE,
    StreamExecGroupWindowTableAggregateRule.INSTANCE,
    // join
    StreamExecJoinRule.INSTANCE,
    StreamExecWindowJoinRule.INSTANCE,
    StreamExecTemporalJoinRule.INSTANCE,
    StreamExecLookupJoinRule.SNAPSHOT_ON_TABLESCAN,
    StreamExecLookupJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    // CEP
    StreamExecMatchRule.INSTANCE,
    // correlate
    StreamExecConstantTableFunctionScanRule.INSTANCE,
    StreamExecCorrelateRule.INSTANCE,
    // sink
    StreamExecSinkRule.INSTANCE
  )

  /**
    * RuleSet for retraction inference.
    */
  val RETRACTION_RULES: RuleSet = RuleSets.ofList(
    // retraction rules
    StreamExecRetractionRules.DEFAULT_RETRACTION_INSTANCE,
    StreamExecRetractionRules.UPDATES_AS_RETRACTION_INSTANCE,
    StreamExecRetractionRules.ACCMODE_INSTANCE
  )

  /**
    * RuleSet related to watermark assignment.
    */
  val MINI_BATCH_RULES: RuleSet = RuleSets.ofList(
    // watermark interval infer rule
    MiniBatchIntervalInferRule.INSTANCE
  )

  /**
    * RuleSet to optimize plans after stream exec execution.
    */
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    //optimize agg rule
    TwoStageOptimizedAggregateRule.INSTANCE,
    // incremental agg rule
    IncrementalAggregateRule.INSTANCE
  )

}
