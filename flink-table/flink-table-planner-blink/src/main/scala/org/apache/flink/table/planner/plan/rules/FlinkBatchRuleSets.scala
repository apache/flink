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
import org.apache.flink.table.planner.plan.rules.physical.batch._

import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.logical.{LogicalIntersect, LogicalMinus, LogicalUnion}
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}

import scala.collection.JavaConverters._

object FlinkBatchRuleSets {

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
    LogicalCorrelateToJoinFromTemporalTableRule.WITHOUT_FILTER)

  val POST_EXPAND_CLEAN_UP_RULES: RuleSet = RuleSets.ofList(
    EnumerableToLogicalTableScan.INSTANCE)

  /**
    * Convert table references before query decorrelation.
    */
  val TABLE_REF_RULES: RuleSet = RuleSets.ofList(
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

  private val LIMIT_RULES: RuleSet = RuleSets.ofList(
    //push down localLimit
    PushLimitIntoLegacyTableSourceScanRule.INSTANCE)

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
    * RuleSet to normalize plans for batch
    */
  val DEFAULT_REWRITE_RULES: RuleSet = RuleSets.ofList((
    PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
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
        ConvertToNotInOrInRule.INSTANCE,
        // optimize limit 0
        FlinkLimit0RemoveRule.INSTANCE,
        // unnest rule
        LogicalUnnestRule.INSTANCE
      )).asJava)

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
    FilterSetOpTransposeRule.INSTANCE,
    FilterMergeRule.INSTANCE
  )

  val JOIN_PREDICATE_REWRITE_RULES: RuleSet = RuleSets.ofList(
    JoinDependentConditionDerivationRule.INSTANCE,
    JoinDeriveNullFilterRule.INSTANCE
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
    ).asJava
  )

  /**
    * RuleSet to do push predicate/partition into table scan
    */
  val FILTER_TABLESCAN_PUSHDOWN_RULES: RuleSet = RuleSets.ofList(
    // push a filter down into the table scan
    PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
    // push partition into the table scan
    PushPartitionIntoLegacyTableSourceScanRule.INSTANCE
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

  val WINDOW_RULES: RuleSet = RuleSets.ofList(
    // slices a project into sections which contain window agg functions and sections which do not.
    ProjectToWindowRule.PROJECT,
    //adjust the sequence of window's groups.
    WindowGroupReorderRule.INSTANCE,
    // Transform window to LogicalWindowAggregate
    WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
    WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE
  )

  val JOIN_COND_EQUAL_TRANSFER_RULES: RuleSet = RuleSets.ofList((
    RuleSets.ofList(JoinConditionEqualityTransferRule.INSTANCE).asScala ++
      PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
      FILTER_RULES.asScala
    ).asJava)

  val JOIN_REORDER_PREPARE_RULES: RuleSet = RuleSets.ofList(
    // merge join to MultiJoin
    JoinToMultiJoinRule.INSTANCE,
    // merge project to MultiJoin
    ProjectMultiJoinMergeRule.INSTANCE,
    // merge filter to MultiJoin
    FilterMultiJoinMergeRule.INSTANCE
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
    PushProjectIntoLegacyTableSourceScanRule.INSTANCE,
    PushFilterIntoLegacyTableSourceScanRule.INSTANCE,

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

    // rank rules
    FlinkLogicalRankRule.CONSTANT_RANGE_INSTANCE,
    // transpose calc past rank to reduce rank input fields
    CalcRankTransposeRule.INSTANCE,
    // remove output of rank number when it is a constant
    RankNumberColumnRemoveRule.INSTANCE,

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
    FlinkLogicalAggregate.BATCH_CONVERTER,
    FlinkLogicalOverAggregate.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalSort.BATCH_CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalLegacyTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalDataStreamTableScan.CONVERTER,
    FlinkLogicalIntermediateTableScan.CONVERTER,
    FlinkLogicalExpand.CONVERTER,
    FlinkLogicalRank.CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalSnapshot.CONVERTER,
    FlinkLogicalSink.CONVERTER,
    FlinkLogicalLegacySink.CONVERTER
  )

  /**
    * RuleSet to do logical optimize for batch
    */
  val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList((
    LIMIT_RULES.asScala ++
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
    // Rule that splits python ScalarFunctions from join conditions
    SplitPythonConditionFromJoinRule.INSTANCE,
    // Rule that splits python ScalarFunctions from
    // java/scala ScalarFunctions in correlate conditions
    SplitPythonConditionFromCorrelateRule.INSTANCE,
    // Rule that transpose the conditions after the Python correlate node.
    CalcPythonCorrelateTransposeRule.INSTANCE,
    // Rule that splits java calls from python TableFunction
    PythonCorrelateSplitRule.INSTANCE,
    // merge calc after calc transpose
    FlinkCalcMergeRule.INSTANCE,
    // Rule that splits python ScalarFunctions from java/scala ScalarFunctions
    PythonCalcSplitRule.SPLIT_CONDITION,
    PythonCalcSplitRule.SPLIT_PROJECT,
    PythonCalcSplitRule.SPLIT_PANDAS_IN_PROJECT,
    PythonCalcSplitRule.EXPAND_PROJECT,
    PythonCalcSplitRule.PUSH_CONDITION,
    PythonCalcSplitRule.REWRITE_PROJECT
  )

  /**
    * RuleSet to do physical optimize for batch
    */
  val PHYSICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.BATCH_INSTANCE,
    // source
    BatchExecBoundedStreamScanRule.INSTANCE,
    BatchExecTableSourceScanRule.INSTANCE,
    BatchExecLegacyTableSourceScanRule.INSTANCE,
    BatchExecIntermediateTableScanRule.INSTANCE,
    BatchExecValuesRule.INSTANCE,
    // calc
    BatchExecCalcRule.INSTANCE,
    BatchExecPythonCalcRule.INSTANCE,
    // union
    BatchExecUnionRule.INSTANCE,
    // sort
    BatchExecSortRule.INSTANCE,
    BatchExecLimitRule.INSTANCE,
    BatchExecSortLimitRule.INSTANCE,
    // rank
    BatchExecRankRule.INSTANCE,
    RemoveRedundantLocalRankRule.INSTANCE,
    // expand
    BatchExecExpandRule.INSTANCE,
    // group agg
    BatchExecHashAggRule.INSTANCE,
    BatchExecSortAggRule.INSTANCE,
    RemoveRedundantLocalSortAggRule.WITHOUT_SORT,
    RemoveRedundantLocalSortAggRule.WITH_SORT,
    RemoveRedundantLocalHashAggRule.INSTANCE,
    // over agg
    BatchExecOverAggregateRule.INSTANCE,
    // window agg
    BatchExecWindowAggregateRule.INSTANCE,
    // join
    BatchExecHashJoinRule.INSTANCE,
    BatchExecSortMergeJoinRule.INSTANCE,
    BatchExecNestedLoopJoinRule.INSTANCE,
    BatchExecSingleRowJoinRule.INSTANCE,
    BatchExecLookupJoinRule.SNAPSHOT_ON_TABLESCAN,
    BatchExecLookupJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    // correlate
    BatchExecConstantTableFunctionScanRule.INSTANCE,
    BatchExecCorrelateRule.INSTANCE,
    BatchExecPythonCorrelateRule.INSTANCE,
    // sink
    BatchExecSinkRule.INSTANCE,
    BatchExecLegacySinkRule.INSTANCE
  )

  /**
    * RuleSet to optimize plans after batch exec execution.
    */
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    EnforceLocalHashAggRule.INSTANCE,
    EnforceLocalSortAggRule.INSTANCE
  )
}
