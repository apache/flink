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
    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
    CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
    CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
  )

  /**
    * Expand plan by replacing references to tables into a proper plan sub trees. Those rules
    * can create new plan nodes.
    */
  val EXPAND_PLAN_RULES: RuleSet = RuleSets.ofList(
    LogicalCorrelateToJoinFromTemporalTableRule.LOOKUP_JOIN_WITH_FILTER,
    LogicalCorrelateToJoinFromTemporalTableRule.LOOKUP_JOIN_WITHOUT_FILTER)

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
    CoreRules.FILTER_REDUCE_EXPRESSIONS,
    CoreRules.PROJECT_REDUCE_EXPRESSIONS,
    CoreRules.CALC_REDUCE_EXPRESSIONS,
    CoreRules.JOIN_REDUCE_EXPRESSIONS
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
    PushLimitIntoTableSourceScanRule.INSTANCE,
    PushLimitIntoLegacyTableSourceScanRule.INSTANCE)

  /**
    * RuleSet to simplify predicate expressions in filters and joins
    */
  private val PREDICATE_SIMPLIFY_EXPRESSION_RULES: RuleSet = RuleSets.ofList(
    SimplifyFilterConditionRule.INSTANCE,
    SimplifyJoinConditionRule.INSTANCE,
    JoinConditionTypeCoerceRule.INSTANCE,
    CoreRules.JOIN_PUSH_EXPRESSIONS
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
        // slices a project into sections which contain window agg functions
        // and sections which do not.
        CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
        // adjust the sequence of window's groups.
        WindowGroupReorderRule.INSTANCE,
        WindowPropertiesRules.WINDOW_PROPERTIES_RULE,
        WindowPropertiesRules.WINDOW_PROPERTIES_HAVING_RULE,
        // let project transpose window operator.
        CoreRules.PROJECT_WINDOW_TRANSPOSE,
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
    CoreRules.FILTER_INTO_JOIN,
    // push filter into the children of a join
    CoreRules.JOIN_CONDITION_PUSH,
    // push filter through an aggregation
    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
    // push a filter past a project
    CoreRules.FILTER_PROJECT_TRANSPOSE,
    CoreRules.FILTER_SET_OP_TRANSPOSE,
    CoreRules.FILTER_MERGE
  )

  val JOIN_NULL_FILTER_RULES: RuleSet = RuleSets.ofList(
    JoinDeriveNullFilterRule.INSTANCE
  )

  val JOIN_PREDICATE_REWRITE_RULES: RuleSet = RuleSets.ofList((
    RuleSets.ofList(JoinDependentConditionDerivationRule.INSTANCE).asScala ++
    JOIN_NULL_FILTER_RULES.asScala
  ).asJava)

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
    PushFilterIntoTableSourceScanRule.INSTANCE,
    PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
    // push partition into the table scan
    PushPartitionIntoLegacyTableSourceScanRule.INSTANCE,
    // push partition into the dynamic table scan
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
    CoreRules.PROJECT_FILTER_TRANSPOSE,
    // push a projection to the children of a non semi/anti join
    // push all expressions to handle the time indicator correctly
    new FlinkProjectJoinTransposeRule(
      PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
    // push a projection to the children of a semi/anti Join
    ProjectSemiAntiJoinTransposeRule.INSTANCE,
    // merge projections
    CoreRules.PROJECT_MERGE,
    // remove identity project
    CoreRules.PROJECT_REMOVE,
    //removes constant keys from an Agg
    CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
    // push project through a Union
    CoreRules.PROJECT_SET_OP_TRANSPOSE
  )

  val JOIN_COND_EQUAL_TRANSFER_RULES: RuleSet = RuleSets.ofList((
    RuleSets.ofList(JoinConditionEqualityTransferRule.INSTANCE).asScala ++
      PREDICATE_SIMPLIFY_EXPRESSION_RULES.asScala ++
      FILTER_RULES.asScala
    ).asJava)

  val JOIN_REORDER_PREPARE_RULES: RuleSet = RuleSets.ofList(
    // merge join to MultiJoin
    CoreRules.JOIN_TO_MULTI_JOIN,
    // merge project to MultiJoin
    CoreRules.PROJECT_MULTI_JOIN_MERGE,
    // merge filter to MultiJoin
    CoreRules.FILTER_MULTI_JOIN_MERGE
  )

  val JOIN_REORDER_RULES: RuleSet = RuleSets.ofList(
    // equi-join predicates transfer
    RewriteMultiJoinConditionRule.INSTANCE,
    // join reorder
    CoreRules.MULTI_JOIN_OPTIMIZE
  )

  /**
    * RuleSet to do logical optimize.
    * This RuleSet is a sub-set of [[LOGICAL_OPT_RULES]].
    */
  private val LOGICAL_RULES: RuleSet = RuleSets.ofList(
    // scan optimization
    PushProjectIntoTableSourceScanRule.INSTANCE,
    PushProjectIntoLegacyTableSourceScanRule.INSTANCE,
    PushFilterIntoTableSourceScanRule.INSTANCE,
    PushFilterIntoLegacyTableSourceScanRule.INSTANCE,
    // transpose project and snapshot for scan optimization
    ProjectSnapshotTransposeRule.INSTANCE,
    // reorder sort and projection
    CoreRules.SORT_PROJECT_TRANSPOSE,
    // remove unnecessary sort rule
    CoreRules.SORT_REMOVE,

    // join rules
    FlinkJoinPushExpressionsRule.INSTANCE,
    SimplifyJoinConditionRule.INSTANCE,

    // remove union with only a single child
    CoreRules.UNION_REMOVE,
    // convert non-all union into all-union + distinct
    CoreRules.UNION_TO_DISTINCT,

    // aggregation and projection rules
    CoreRules.AGGREGATE_PROJECT_MERGE,
    CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,

    // remove aggregation if it does not aggregate and input is already distinct
    FlinkAggregateRemoveRule.INSTANCE,
    // push aggregate through join
    FlinkAggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    CoreRules.AGGREGATE_UNION_AGGREGATE,
    // expand distinct aggregate to normal aggregate with groupby
    FlinkAggregateExpandDistinctAggregatesRule.INSTANCE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
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
    ConstantRankNumberColumnRemoveRule.INSTANCE,

    // calc rules
    CoreRules.FILTER_CALC_MERGE,
    CoreRules.PROJECT_CALC_MERGE,
    CoreRules.FILTER_TO_CALC,
    CoreRules.PROJECT_TO_CALC,
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
    FlinkLogicalLegacySink.CONVERTER,
    FlinkLogicalDistribution.BATCH_CONVERTER
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
    PythonCalcSplitRule.SPLIT_CONDITION_REX_FIELD,
    PythonCalcSplitRule.SPLIT_PROJECTION_REX_FIELD,
    PythonCalcSplitRule.SPLIT_CONDITION,
    PythonCalcSplitRule.SPLIT_PROJECT,
    PythonCalcSplitRule.SPLIT_PANDAS_IN_PROJECT,
    PythonCalcSplitRule.EXPAND_PROJECT,
    PythonCalcSplitRule.PUSH_CONDITION,
    PythonCalcSplitRule.REWRITE_PROJECT,
    PythonMapMergeRule.INSTANCE,
    // remove output of rank number when it is not used by successor calc
    RedundantRankNumberColumnRemoveRule.INSTANCE
    )

  /**
    * RuleSet to do physical optimize for batch
    */
  val PHYSICAL_OPT_RULES: RuleSet = RuleSets.ofList(
    FlinkExpandConversionRule.BATCH_INSTANCE,
    // source
    BatchPhysicalBoundedStreamScanRule.INSTANCE,
    BatchPhysicalTableSourceScanRule.INSTANCE,
    BatchPhysicalLegacyTableSourceScanRule.INSTANCE,
    BatchPhysicalIntermediateTableScanRule.INSTANCE,
    BatchPhysicalValuesRule.INSTANCE,
    // calc
    BatchPhysicalCalcRule.INSTANCE,
    BatchPhysicalPythonCalcRule.INSTANCE,
    // union
    BatchPhysicalUnionRule.INSTANCE,
    // sort
    BatchPhysicalSortRule.INSTANCE,
    BatchPhysicalLimitRule.INSTANCE,
    BatchPhysicalSortLimitRule.INSTANCE,
    // rank
    BatchPhysicalRankRule.INSTANCE,
    RemoveRedundantLocalRankRule.INSTANCE,
    // expand
    BatchPhysicalExpandRule.INSTANCE,
    // group agg
    BatchPhysicalHashAggRule.INSTANCE,
    BatchPhysicalSortAggRule.INSTANCE,
    RemoveRedundantLocalSortAggRule.WITHOUT_SORT,
    RemoveRedundantLocalSortAggRule.WITH_SORT,
    RemoveRedundantLocalHashAggRule.INSTANCE,
    BatchPhysicalPythonAggregateRule.INSTANCE,
    // over agg
    BatchPhysicalOverAggregateRule.INSTANCE,
    // window agg
    BatchPhysicalWindowAggregateRule.INSTANCE,
    BatchPhysicalPythonWindowAggregateRule.INSTANCE,
    // join
    BatchPhysicalHashJoinRule.INSTANCE,
    BatchPhysicalSortMergeJoinRule.INSTANCE,
    BatchPhysicalNestedLoopJoinRule.INSTANCE,
    BatchPhysicalSingleRowJoinRule.INSTANCE,
    BatchPhysicalLookupJoinRule.SNAPSHOT_ON_TABLESCAN,
    BatchPhysicalLookupJoinRule.SNAPSHOT_ON_CALC_TABLESCAN,
    // correlate
    BatchPhysicalConstantTableFunctionScanRule.INSTANCE,
    BatchPhysicalCorrelateRule.INSTANCE,
    BatchPhysicalPythonCorrelateRule.INSTANCE,
    // sink
    BatchPhysicalSinkRule.INSTANCE,
    BatchPhysicalLegacySinkRule.INSTANCE,
    // hive distribution
    BatchPhysicalDistributionRule.INSTANCE
  )

  /**
    * RuleSet to optimize plans after batch exec execution.
    */
  val PHYSICAL_REWRITE: RuleSet = RuleSets.ofList(
    EnforceLocalHashAggRule.INSTANCE,
    EnforceLocalSortAggRule.INSTANCE
  )
}
