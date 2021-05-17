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
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.{RuleSet, RuleSets}
import org.apache.flink.table.plan.nodes.logical._
import org.apache.flink.table.plan.rules.common._
import org.apache.flink.table.plan.rules.dataSet._
import org.apache.flink.table.plan.rules.datastream._
import org.apache.flink.table.plan.rules.logical.{ExtendedAggregateExtractProjectRule, _}
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalProject}
import org.apache.flink.table.plan.rules.batch.DataSetPythonCorrelateRule
import org.apache.flink.table.plan.rules.stream.DataStreamPythonCorrelateRule

object FlinkRuleSets {

  /**
    * Convert sub-queries before query decorrelation.
    */
  val TABLE_SUBQUERY_RULES: RuleSet = RuleSets.ofList(
    CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
    CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
    CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)

  /**
    * Expand plan by replacing references to tables into a proper plan sub trees. Those rules
    * can create new plan nodes.
    */
  val EXPAND_PLAN_RULES: RuleSet = RuleSets.ofList(
    LogicalCorrelateToTemporalTableJoinRule.INSTANCE)

  val POST_EXPAND_CLEAN_UP_RULES: RuleSet = RuleSets.ofList(
    EnumerableToLogicalTableScan.INSTANCE)

  val LOGICAL_OPT_RULES: RuleSet = RuleSets.ofList(

    // push a filter into a join
    CoreRules.FILTER_INTO_JOIN,
    // push filter into the children of a join
    CoreRules.JOIN_CONDITION_PUSH,
    // push filter through an aggregation
    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
    // push filter through set operation
    CoreRules.FILTER_SET_OP_TRANSPOSE,
    // push project through set operation
    CoreRules.PROJECT_SET_OP_TRANSPOSE,

    // aggregation and projection rules
    CoreRules.AGGREGATE_PROJECT_MERGE,
    CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
    // push a projection past a filter or vice versa
    CoreRules.PROJECT_FILTER_TRANSPOSE,
    CoreRules.FILTER_PROJECT_TRANSPOSE,
    // push a projection to the children of a join
    // push all expressions to handle the time indicator correctly
    new ProjectJoinTransposeRule(
      classOf[LogicalProject],
      classOf[LogicalJoin],
      PushProjector.ExprCondition.FALSE,
      RelFactories.LOGICAL_BUILDER),
    // merge projections
    CoreRules.PROJECT_MERGE,
    // remove identity project
    CoreRules.PROJECT_REMOVE,
    // reorder sort and projection
    CoreRules.SORT_PROJECT_TRANSPOSE,

    // join rules
    CoreRules.JOIN_PUSH_EXPRESSIONS,

    // remove union with only a single child
    CoreRules.UNION_REMOVE,
    // convert non-all union into all-union + distinct
    CoreRules.UNION_TO_DISTINCT,

    // remove aggregation if it does not aggregate and input is already distinct
    CoreRules.AGGREGATE_REMOVE,
    // push aggregate through join
    CoreRules.AGGREGATE_JOIN_TRANSPOSE,
    // aggregate union rule
    CoreRules.AGGREGATE_UNION_AGGREGATE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // remove unnecessary sort rule
    CoreRules.SORT_REMOVE,

    // prune empty results rules
    PruneEmptyRules.AGGREGATE_INSTANCE,
    PruneEmptyRules.FILTER_INSTANCE,
    PruneEmptyRules.JOIN_LEFT_INSTANCE,
    PruneEmptyRules.JOIN_RIGHT_INSTANCE,
    PruneEmptyRules.PROJECT_INSTANCE,
    PruneEmptyRules.SORT_INSTANCE,
    PruneEmptyRules.UNION_INSTANCE,

    // calc rules
    CoreRules.FILTER_CALC_MERGE,
    CoreRules.PROJECT_CALC_MERGE,
    CoreRules.FILTER_TO_CALC,
    CoreRules.PROJECT_TO_CALC,
    CoreRules.CALC_MERGE,

    // scan optimization
    PushProjectIntoTableSourceScanRule.INSTANCE,
    PushFilterIntoTableSourceScanRule.INSTANCE,

    // unnest rule
    LogicalUnnestRule.INSTANCE,

    // translate to flink logical rel nodes
    FlinkLogicalAggregate.CONVERTER,
    FlinkLogicalWindowAggregate.CONVERTER,
    FlinkLogicalOverWindow.CONVERTER,
    FlinkLogicalCalc.CONVERTER,
    FlinkLogicalCorrelate.CONVERTER,
    FlinkLogicalIntersect.CONVERTER,
    FlinkLogicalJoin.CONVERTER,
    FlinkLogicalTemporalTableJoin.CONVERTER,
    FlinkLogicalMinus.CONVERTER,
    FlinkLogicalSort.CONVERTER,
    FlinkLogicalUnion.CONVERTER,
    FlinkLogicalValues.CONVERTER,
    FlinkLogicalTableSourceScan.CONVERTER,
    FlinkLogicalTableFunctionScan.CONVERTER,
    FlinkLogicalMatch.CONVERTER,
    FlinkLogicalTableAggregate.CONVERTER,
    FlinkLogicalWindowTableAggregate.CONVERTER,
    FlinkLogicalSink.CONVERTER
  )

  /**
    * RuleSet to do rewrite on FlinkLogicalRel
    */
  val LOGICAL_REWRITE_RULES: RuleSet = RuleSets.ofList(
    // Rule that splits python ScalarFunctions from join conditions
    SplitPythonConditionFromJoinRule.INSTANCE,
    // Rule that splits python ScalarFunctions from
    // java/scala ScalarFunctions in correlate conditions
    SplitPythonConditionFromCorrelateRule.INSTANCE,
    // Rule that transpose the conditions after the Python correlate node.
    CalcPythonCorrelateTransposeRule.INSTANCE,
    // Rule that splits java calls from python TableFunction
    PythonCorrelateSplitRule.INSTANCE,
    CoreRules.CALC_MERGE,
    PythonCalcSplitRule.SPLIT_CONDITION,
    PythonCalcSplitRule.SPLIT_PROJECT,
    PythonCalcSplitRule.SPLIT_PANDAS_IN_PROJECT,
    PythonCalcSplitRule.EXPAND_PROJECT,
    PythonCalcSplitRule.PUSH_CONDITION,
    PythonCalcSplitRule.REWRITE_PROJECT
  )

  /**
    * RuleSet to normalize plans for batch / DataSet execution
    */
  val DATASET_NORM_RULES: RuleSet = RuleSets.ofList(
    CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,

    // Transform grouping sets
    DecomposeGroupingSetRule.INSTANCE,
    // Transform window to LogicalWindowAggregate
    DataSetLogicalWindowAggregateRule.INSTANCE,
    WindowPropertiesRule.INSTANCE,
    WindowPropertiesHavingRule.INSTANCE,

    // expand distinct aggregate to normal aggregate with groupby
    CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN,

    ExtendedAggregateExtractProjectRule.INSTANCE,
    // simplify expressions rules
    CoreRules.FILTER_REDUCE_EXPRESSIONS,
    CoreRules.PROJECT_REDUCE_EXPRESSIONS,
    CoreRules.CALC_REDUCE_EXPRESSIONS,
    CoreRules.JOIN_REDUCE_EXPRESSIONS,

    // merge a cascade of predicates to IN or NOT_IN
    ConvertToNotInOrInRule.IN_INSTANCE,
    ConvertToNotInOrInRule.NOT_IN_INSTANCE
  )

  /**
    * RuleSet to optimize plans for batch / DataSet execution
    */
  val DATASET_OPT_RULES: RuleSet = RuleSets.ofList(
    // translate to Flink DataSet nodes
    DataSetWindowAggregateRule.INSTANCE,
    DataSetAggregateRule.INSTANCE,
    DataSetDistinctRule.INSTANCE,
    DataSetCalcRule.INSTANCE,
    DataSetPythonCalcRule.INSTANCE,
    DataSetJoinRule.INSTANCE,
    DataSetSingleRowJoinRule.INSTANCE,
    DataSetScanRule.INSTANCE,
    DataSetUnionRule.INSTANCE,
    DataSetIntersectRule.INSTANCE,
    DataSetMinusRule.INSTANCE,
    DataSetSortRule.INSTANCE,
    DataSetValuesRule.INSTANCE,
    DataSetCorrelateRule.INSTANCE,
    DataSetPythonCorrelateRule.INSTANCE,
    BatchTableSourceScanRule.INSTANCE,
    DataSetSinkRule.INSTANCE
  )

  /**
    * RuleSet to normalize plans for stream / DataStream execution
    */
  val DATASTREAM_NORM_RULES: RuleSet = RuleSets.ofList(
    // Transform window to LogicalWindowAggregate
    DataStreamLogicalWindowAggregateRule.INSTANCE,
    WindowPropertiesRule.INSTANCE,
    WindowPropertiesHavingRule.INSTANCE,

    ExtendedAggregateExtractProjectRule.INSTANCE,
    // simplify expressions rules
    CoreRules.FILTER_REDUCE_EXPRESSIONS,
    CoreRules.PROJECT_REDUCE_EXPRESSIONS,
    CoreRules.CALC_REDUCE_EXPRESSIONS,
    CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,

    // merge a cascade of predicates to IN or NOT_IN
    ConvertToNotInOrInRule.IN_INSTANCE,
    ConvertToNotInOrInRule.NOT_IN_INSTANCE
  )

  /**
    * RuleSet to optimize plans for stream / DataStream execution
    */
  val DATASTREAM_OPT_RULES: RuleSet = RuleSets.ofList(
    // translate to DataStream nodes
    DataStreamSortRule.INSTANCE,
    DataStreamGroupAggregateRule.INSTANCE,
    DataStreamOverAggregateRule.INSTANCE,
    DataStreamGroupWindowAggregateRule.INSTANCE,
    DataStreamCalcRule.INSTANCE,
    DataStreamScanRule.INSTANCE,
    DataStreamUnionRule.INSTANCE,
    DataStreamValuesRule.INSTANCE,
    DataStreamCorrelateRule.INSTANCE,
    DataStreamWindowJoinRule.INSTANCE,
    DataStreamJoinRule.INSTANCE,
    DataStreamTemporalTableJoinRule.INSTANCE,
    StreamTableSourceScanRule.INSTANCE,
    DataStreamMatchRule.INSTANCE,
    DataStreamTableAggregateRule.INSTANCE,
    DataStreamGroupWindowTableAggregateRule.INSTANCE,
    DataStreamPythonCalcRule.INSTANCE,
    DataStreamPythonCorrelateRule.INSTANCE,
    DataStreamSinkRule.INSTANCE
  )

  /**
    * RuleSet to decorate plans for stream / DataStream execution
    */
  val DATASTREAM_DECO_RULES: RuleSet = RuleSets.ofList(
    // retraction rules
    DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE,
    DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE,
    DataStreamRetractionRules.ACCMODE_INSTANCE
  )

}
