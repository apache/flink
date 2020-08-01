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
    SubQueryRemoveRule.FILTER,
    SubQueryRemoveRule.PROJECT,
    SubQueryRemoveRule.JOIN)

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
    FilterJoinRule.FILTER_ON_JOIN,
    // push filter into the children of a join
    FilterJoinRule.JOIN,
    // push filter through an aggregation
    FilterAggregateTransposeRule.INSTANCE,
    // push filter through set operation
    FilterSetOpTransposeRule.INSTANCE,
    // push project through set operation
    ProjectSetOpTransposeRule.INSTANCE,

    // aggregation and projection rules
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    // push a projection past a filter or vice versa
    ProjectFilterTransposeRule.INSTANCE,
    FilterProjectTransposeRule.INSTANCE,
    // push a projection to the children of a join
    // push all expressions to handle the time indicator correctly
    new ProjectJoinTransposeRule(
      classOf[LogicalProject],
      classOf[LogicalJoin],
      PushProjector.ExprCondition.FALSE,
      RelFactories.LOGICAL_BUILDER),
    // merge projections
    ProjectMergeRule.INSTANCE,
    // remove identity project
    ProjectRemoveRule.INSTANCE,
    // reorder sort and projection
    SortProjectTransposeRule.INSTANCE,
    ProjectSortTransposeRule.INSTANCE,

    // join rules
    JoinPushExpressionsRule.INSTANCE,

    // remove union with only a single child
    UnionEliminatorRule.INSTANCE,
    // convert non-all union into all-union + distinct
    UnionToDistinctRule.INSTANCE,

    // remove aggregation if it does not aggregate and input is already distinct
    AggregateRemoveRule.INSTANCE,
    // push aggregate through join
    AggregateJoinTransposeRule.EXTENDED,
    // aggregate union rule
    AggregateUnionAggregateRule.INSTANCE,

    // reduce aggregate functions like AVG, STDDEV_POP etc.
    AggregateReduceFunctionsRule.INSTANCE,
    WindowAggregateReduceFunctionsRule.INSTANCE,

    // remove unnecessary sort rule
    SortRemoveRule.INSTANCE,

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
    CalcMergeRule.INSTANCE,
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
    ProjectToWindowRule.PROJECT,

    // Transform grouping sets
    DecomposeGroupingSetRule.INSTANCE,
    // Transform window to LogicalWindowAggregate
    DataSetLogicalWindowAggregateRule.INSTANCE,
    WindowPropertiesRule.INSTANCE,
    WindowPropertiesHavingRule.INSTANCE,

    // expand distinct aggregate to normal aggregate with groupby
    AggregateExpandDistinctAggregatesRule.JOIN,

    ExtendedAggregateExtractProjectRule.INSTANCE,
    // simplify expressions rules
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ReduceExpressionsRule.JOIN_INSTANCE,

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
    ReduceExpressionsRule.FILTER_INSTANCE,
    ReduceExpressionsRule.PROJECT_INSTANCE,
    ReduceExpressionsRule.CALC_INSTANCE,
    ProjectToWindowRule.PROJECT,

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
