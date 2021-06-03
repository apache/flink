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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkLogicalRelFactories, FlinkRelBuilder}
import org.apache.flink.table.planner.functions.sql.{FlinkSqlOperatorTable, SqlFirstLastValueAggFunction}
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.utils.AggregateUtil.doAllAggSupportSplit
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ExpandUtil, WindowUtil}

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.fun.{SqlMinMaxAggFunction, SqlStdOperatorTable}
import org.apache.calcite.sql.{SqlAggFunction, SqlKind}
import org.apache.calcite.util.{ImmutableBitSet, ImmutableIntList}

import com.google.common.collect.ImmutableList

import java.math.{BigDecimal => JBigDecimal}
import java.util

import scala.collection.JavaConversions._

/**
  * Planner rule that splits aggregations containing distinct aggregates, e.g, count distinct,
  * into partial aggregations and final aggregations.
  *
  * This rule rewrites an aggregate query with distinct aggregations into an expanded
  * double aggregations. The first aggregation compute the results in sub-partition and
  * the results are combined by the second aggregation.
  *
  * Examples:
  *
  * MyTable: a: BIGINT, b: INT, c: VARCHAR
  *
  * Original records:
  * +-----+-----+-----+
  * |  a  |  b  |  c  |
  * +-----+-----+-----+
  * |  1  |  1  |  c1 |
  * +-----+-----+-----+
  * |  1  |  2  |  c1 |
  * +-----+-----+-----+
  * |  2  |  1  |  c2 |
  * +-----+-----+-----+
  *
  * SQL:
  * SELECT SUM(DISTINCT b), COUNT(DISTINCT c), AVG(b) FROM MyTable GROUP BY a
  *
  * flink logical plan:
  * {{{
  * FlinkLogicalCalc(select=[$f1 AS EXPR$0, $f2 AS EXPR$1, CAST(IF(=($f4, 0:BIGINT), null:INTEGER,
  *  /($f3, $f4))) AS EXPR$2])
  * +- FlinkLogicalAggregate(group=[{0}], agg#0=[SUM($3)], agg#1=[$SUM0($4)], agg#2=[$SUM0($5)],
  * agg#3=[$SUM0($6)])
  *    +- FlinkLogicalAggregate(group=[{0, 3, 4}], agg#0=[SUM(DISTINCT $1) FILTER $5],
  *    agg#1=[COUNT(DISTINCT $2) FILTER $6], agg#2=[$SUM0($1) FILTER $7],
  *    agg#3=[COUNT($1) FILTER $7])
  *       +- FlinkLogicalCalc(select=[a, b, c, $f3, $f4, =($e, 1) AS $g_1, =($e, 2) AS $g_2,
  *       =($e, 3) AS $g_3])
  *          +- FlinkLogicalExpand(projects=[a, b, c, $f3, $f4, $e])
  *             +- FlinkLogicalCalc(select=[a, b, c, MOD(HASH_CODE(b), 1024) AS $f3,
  *               MOD(HASH_CODE(c), 1024) AS $f4])
  *                +- FlinkLogicalTableSourceScan(table=[[MyTable,
  *                       source: [TestTableSource(a, b, c)]]], fields=[a, b, c])
  * }}}
  *
  * '$e = 1' is equivalent to 'group by a, hash(b) % 1024'
  * '$e = 2' is equivalent to 'group by a, hash(c) % 1024'
  * '$e = 3' is equivalent to 'group by a
  *
  * Expanded records:
  * +-----+-----+-----+------------------+------------------+-----+
  * |  a  |  b  |  c  |  hash(b) % 1024  |  hash(c) % 1024  | $e  |
  * +-----+-----+-----+------------------+------------------+-----+               ---+---
  * |  1  |  1  |  c1 | hash(b) % 1024   |  null            | 1   |                  |
  * +-----+-----+-----+------------------+------------------+-----+                  |
  * |  1  |  1  |  c1 | null             |  hash(c) % 1024 |  2   |    records expanded by record1
  * +-----+-----+-----+------------------+-----------------+------+                  |
  * |  1  |  1  |  c1 | null             |  null           |  3   |                  |
  * +-----+-----+-----+------------------+-----------------+------+               ---+---
  * |  1  |  2  |  c1 | hash(b) % 1024   |  null           |  1   |                  |
  * +-----+-----+-----+------------------+-----------------+------+                  |
  * |  1  |  2  |  c1 | null             |  hash(c) % 1024 |  2   |    records expanded by record2
  * +-----+-----+-----+------------------+-----------------+------+                  |
  * |  1  |  2  |  c1 | null             |  null           |  3   |                  |
  * +-----+-----+-----+------------------+-----------------+------+               ---+---
  * |  2  |  1  |  c2 | hash(b) % 1024   |  null           |  1   |                  |
  * +-----+-----+-----+------------------+-----------------+------+                  |
  * |  2  |  1  |  c2 | null             |  hash(c) % 1024 |  2   |    records expanded by record3
  * +-----+-----+-----+------------------+-----------------+------+                  |
  * |  2  |  1  |  c2 |  null            |  null           |  3   |                  |
  * +-----+-----+-----+------------------+-----------------+------+               ---+---
  *
  * NOTES: this rule is only used for Stream now.
  */
class SplitAggregateRule extends RelOptRule(
  operand(classOf[FlinkLogicalAggregate],
    operand(classOf[FlinkRelNode], any)),
  FlinkLogicalRelFactories.FLINK_LOGICAL_REL_BUILDER,
  "SplitAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val agg: FlinkLogicalAggregate = call.rel(0)

    val splitDistinctAggEnabled = tableConfig.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED)
    val isAllAggSplittable = doAllAggSupportSplit(agg.getAggCallList)

    // disable distinct split for processing-time window,
    // because the semantic is not clear to materialize processing-time window in two aggregates
    val fmq = call.getMetadataQuery.asInstanceOf[FlinkRelMetadataQuery]
    val windowProps = fmq.getRelWindowProperties(agg.getInput)
    val isWindowAgg = WindowUtil.groupingContainsWindowStartEnd(agg.getGroupSet, windowProps)
    val isProctimeWindowAgg = isWindowAgg && !windowProps.isRowtime
    // TableAggregate is not supported. see also FLINK-21923.
    val isTableAgg = AggregateUtil.isTableAggregate(agg.getAggCallList)

    agg.partialFinalType == PartialFinalType.NONE && agg.containsDistinctCall() &&
      splitDistinctAggEnabled && isAllAggSplittable && !isProctimeWindowAgg && !isTableAgg
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val originalAggregate: FlinkLogicalAggregate = call.rel(0)
    val aggCalls = originalAggregate.getAggCallList
    val input: FlinkRelNode = call.rel(1)
    val cluster = originalAggregate.getCluster
    val relBuilder = call.builder().asInstanceOf[FlinkRelBuilder]
    relBuilder.push(input)
    val aggGroupSet = originalAggregate.getGroupSet.toArray

    // STEP 1: add hash fields if necessary
    val hashFieldIndexes: Array[Int] = aggCalls.flatMap { aggCall =>
      if (SplitAggregateRule.needAddHashFields(aggCall)) {
        SplitAggregateRule.getArgIndexes(aggCall)
      } else {
        Array.empty[Int]
      }
    }.distinct.diff(aggGroupSet).sorted.toArray

    val hashFieldsMap: util.Map[Int, Int] = new util.HashMap()
    val buckets = tableConfig.getConfiguration.getInteger(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM)

    if (hashFieldIndexes.nonEmpty) {
      val projects = new util.ArrayList[RexNode](relBuilder.fields)
      val hashFieldsOffset = projects.size()

      hashFieldIndexes.zipWithIndex.foreach { case (hashFieldIdx, index) =>
        val hashField = relBuilder.field(hashFieldIdx)
        // hash(f) % buckets
        val node: RexNode = relBuilder.call(
          SqlStdOperatorTable.MOD,
          relBuilder.call(FlinkSqlOperatorTable.HASH_CODE, hashField),
          relBuilder.literal(buckets))
        projects.add(node)
        hashFieldsMap.put(hashFieldIdx, hashFieldsOffset + index)
      }
      relBuilder.project(projects)
    }

    // STEP 2: construct partial aggregates
    val groupSetTreeSet = new util.TreeSet[ImmutableBitSet](ImmutableBitSet.ORDERING)
    val aggInfoToGroupSetMap = new util.HashMap[AggregateCall, ImmutableBitSet]()
    var newGroupSetsNum = 0
    aggCalls.foreach { aggCall =>
      val groupSet = if (SplitAggregateRule.needAddHashFields(aggCall)) {
        val newIndexes = SplitAggregateRule.getArgIndexes(aggCall).map { argIndex =>
          hashFieldsMap.getOrElse(argIndex, argIndex).asInstanceOf[Integer]
        }.toSeq
        val newGroupSet = ImmutableBitSet.of(newIndexes).union(ImmutableBitSet.of(aggGroupSet: _*))
        // Only increment groupSet number if aggregate call needs add new different hash fields
        // e.g SQL1: SELECT COUNT(DISTINCT a), MAX(a) FROM T group by b
        // newGroupSetsNum is 1 because two agg function add same hash field
        // e.g SQL2: SELECT COUNT(DISTINCT a), COUNT(b) FROM T group by c
        // newGroupSetsNum is 1 because only COUNT(DISTINCT a) adds a new hash field
        // e.g SQL3: SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM T group by b
        // newGroupSetsNum is 2 because COUNT(DISTINCT a), COUNT(DISTINCT b) both add hash field
        if (!groupSetTreeSet.contains(newGroupSet)) {
          newGroupSetsNum += 1
        }
        newGroupSet
      } else {
        ImmutableBitSet.of(aggGroupSet: _*)
      }
      groupSetTreeSet.add(groupSet)
      aggInfoToGroupSetMap.put(aggCall, groupSet)
    }
    val groupSets = ImmutableList.copyOf(asJavaIterable(groupSetTreeSet))
    val fullGroupSet = ImmutableBitSet.union(groupSets)

    // STEP 2.1: expand input fields
    val partialAggCalls = new util.ArrayList[AggregateCall]
    val partialAggCallToGroupSetMap = new util.HashMap[AggregateCall, ImmutableBitSet]()
    aggCalls.foreach { aggCall =>
      val newAggCalls = SplitAggregateRule.getPartialAggFunction(aggCall).map { aggFunc =>
        AggregateCall.create(aggFunc, aggCall.isDistinct, aggCall.isApproximate, aggCall.getArgList,
          aggCall.filterArg, fullGroupSet.cardinality, relBuilder.peek(), null, null)
      }
      partialAggCalls.addAll(newAggCalls)
      newAggCalls.foreach { newAggCall =>
        partialAggCallToGroupSetMap.put(newAggCall, aggInfoToGroupSetMap.get(aggCall))
      }
    }

    val needExpand = newGroupSetsNum > 1
    val duplicateFieldMap = if (needExpand) {
      val (duplicateFieldMap, _) = ExpandUtil.buildExpandNode(
        relBuilder, partialAggCalls, fullGroupSet, groupSets)
      duplicateFieldMap
    } else {
      Map.empty[Integer, Integer]
    }

    // STEP 2.2: add filter columns for partial aggregates
    val filters = new util.LinkedHashMap[(ImmutableBitSet, Integer), Integer]
    val newPartialAggCalls = new util.ArrayList[AggregateCall]
    if (needExpand) {
      // GROUPING returns an integer (0, 1, 2...).
      // Add a project to convert those values to BOOLEAN.
      val nodes = new util.ArrayList[RexNode](relBuilder.fields)
      val expandIdNode = nodes.remove(nodes.size - 1)
      val filterColumnsOffset: Int = nodes.size
      var x: Int = 0
      partialAggCalls.foreach { aggCall =>
        val groupSet = partialAggCallToGroupSetMap.get(aggCall)
        val oldFilterArg = aggCall.filterArg
        val newArgList = aggCall.getArgList.map(a => duplicateFieldMap.getOrElse(a, a)).toList

        if (!filters.contains(groupSet, oldFilterArg)) {
          val expandId = ExpandUtil.genExpandId(fullGroupSet, groupSet)
          if (oldFilterArg >= 0) {
            nodes.add(relBuilder.alias(
              relBuilder.and(
                relBuilder.equals(expandIdNode, relBuilder.literal(expandId)),
                relBuilder.field(oldFilterArg)),
              "$g_" + expandId))
          } else {
            nodes.add(relBuilder.alias(
              relBuilder.equals(expandIdNode, relBuilder.literal(expandId)), "$g_" + expandId))
          }
          val newFilterArg = filterColumnsOffset + x
          filters.put((groupSet, oldFilterArg), newFilterArg)
          x += 1
        }

        val newFilterArg = filters((groupSet, oldFilterArg))
        val newAggCall = aggCall.adaptTo(
          relBuilder.peek(), newArgList, newFilterArg,
          fullGroupSet.cardinality, fullGroupSet.cardinality)
        newPartialAggCalls.add(newAggCall)
      }
      relBuilder.project(nodes)
    } else {
      newPartialAggCalls.addAll(partialAggCalls)
    }

    // STEP 2.3: construct partial aggregates
    // Create aggregate node directly to avoid ClassCastException,
    // Please see FLINK-21923 for more details.
    // TODO reuse aggregate function, see FLINK-22412
    val partialAggregate = FlinkLogicalAggregate.create(
      relBuilder.build(),
      fullGroupSet,
      ImmutableList.of[ImmutableBitSet](fullGroupSet),
      newPartialAggCalls)
    partialAggregate.setPartialFinalType(PartialFinalType.PARTIAL)
    relBuilder.push(partialAggregate)

    // STEP 3: construct final aggregates
    val finalAggInputOffset = fullGroupSet.cardinality
    var x: Int = 0
    val finalAggCalls = new util.ArrayList[AggregateCall]
    var needMergeFinalAggOutput: Boolean = false
    aggCalls.foreach { aggCall =>
      val newAggCalls = SplitAggregateRule.getFinalAggFunction(aggCall).map { aggFunction =>
        val newArgList = ImmutableIntList.of(finalAggInputOffset + x)
        x += 1

        AggregateCall.create(
          aggFunction, false, aggCall.isApproximate, newArgList, -1,
          originalAggregate.getGroupCount, relBuilder.peek(), null, null)
      }

      finalAggCalls.addAll(newAggCalls)
      if (newAggCalls.size > 1) {
        needMergeFinalAggOutput = true
      }
    }
    // Create aggregate node directly to avoid ClassCastException,
    // Please see FLINK-21923 for more details.
    // TODO reuse aggregate function, see FLINK-22412
    val finalAggregate = FlinkLogicalAggregate.create(
      relBuilder.build(),
      SplitAggregateRule.remap(fullGroupSet, originalAggregate.getGroupSet),
      SplitAggregateRule.remap(fullGroupSet, Seq(originalAggregate.getGroupSet)),
      finalAggCalls)
    finalAggregate.setPartialFinalType(PartialFinalType.FINAL)
    relBuilder.push(finalAggregate)

    // STEP 4: convert final aggregation output to the original aggregation output.
    // For example, aggregate function AVG is transformed to SUM0 and COUNT, so the output of
    // the final aggregation is (sum, count). We should converted it to (sum / count)
    // for the final output.
    val aggGroupCount = finalAggregate.getGroupCount
    if (needMergeFinalAggOutput) {
      val nodes = new util.ArrayList[RexNode]
      (0 until aggGroupCount).foreach { index =>
        nodes.add(RexInputRef.of(index, finalAggregate.getRowType))
      }

      var avgAggCount: Int = 0
      aggCalls.zipWithIndex.foreach { case (aggCall, index) =>
        val newNode = if (aggCall.getAggregation.getKind == SqlKind.AVG) {
          val sumInputRef = RexInputRef.of(
            aggGroupCount + index + avgAggCount,
            finalAggregate.getRowType)
          val countInputRef = RexInputRef.of(
            aggGroupCount + index + avgAggCount + 1,
            finalAggregate.getRowType)
          avgAggCount += 1
          // Make a guarantee that the final aggregation returns NULL if underlying count is ZERO.
          // We use SUM0 for underlying sum, which may run into ZERO / ZERO,
          // and division by zero exception occurs.
          // @see Glossary#SQL2011 SQL:2011 Part 2 Section 6.27
          val equals = relBuilder.call(
            FlinkSqlOperatorTable.EQUALS,
            countInputRef,
            relBuilder.getRexBuilder.makeBigintLiteral(JBigDecimal.valueOf(0)))
          val ifTrue = relBuilder.cast(
            relBuilder.getRexBuilder.constantNull(), aggCall.`type`.getSqlTypeName)
          val ifFalse = relBuilder.call(FlinkSqlOperatorTable.DIVIDE, sumInputRef, countInputRef)
          relBuilder.call(
            FlinkSqlOperatorTable.IF,
            equals,
            ifTrue,
            ifFalse)
        } else {
          RexInputRef.of(aggGroupCount + index + avgAggCount, finalAggregate.getRowType)
        }
        nodes.add(newNode)
      }
      relBuilder.project(nodes)
    }

    relBuilder.convert(originalAggregate.getRowType, false)

    val newRel = relBuilder.build()
    call.transformTo(newRel)
  }
}

object SplitAggregateRule {
  val INSTANCE: RelOptRule = new SplitAggregateRule

  // mapping aggFun to (partial aggFun, final aggFun)
  val PARTIAL_FINAL_MAP: Map[SqlAggFunction, (Seq[SqlAggFunction], Seq[SqlAggFunction])] = Map(
    AVG -> (Seq(SUM0, COUNT), Seq(SUM0, SUM0)),
    COUNT -> (Seq(COUNT), Seq(SUM0)),
    MIN -> (Seq(MIN), Seq(MIN)),
    MAX -> (Seq(MAX), Seq(MAX)),
    SUM -> (Seq(SUM), Seq(SUM)),
    SUM0 -> (Seq(SUM0), Seq(SUM0)),
    FlinkSqlOperatorTable.FIRST_VALUE ->
      (Seq(FlinkSqlOperatorTable.FIRST_VALUE), Seq(FlinkSqlOperatorTable.FIRST_VALUE)),
    FlinkSqlOperatorTable.LAST_VALUE ->
      (Seq(FlinkSqlOperatorTable.LAST_VALUE), Seq(FlinkSqlOperatorTable.LAST_VALUE)),
    FlinkSqlOperatorTable.LISTAGG ->
      (Seq(FlinkSqlOperatorTable.LISTAGG), Seq(FlinkSqlOperatorTable.LISTAGG)),
    SINGLE_VALUE -> (Seq(SINGLE_VALUE), Seq(SINGLE_VALUE))
  )

  private def needAddHashFields(aggCall: AggregateCall): Boolean = {
    // When min/max/first_value/last_value is in retraction mode, records will aggregate into
    // one single operator instance regardless of localAgg optimization, which leads to hotspot.
    // So we split them into partial/final aggs either.
    val needSplit = aggCall.getAggregation match {
      case _: SqlMinMaxAggFunction | _: SqlFirstLastValueAggFunction => true
      case _ => false
    }
    needSplit || aggCall.isDistinct
  }

  private def getArgIndexes(aggCall: AggregateCall): Array[Int] = {
    aggCall.getArgList.map(_.intValue()).toArray
  }

  private def getPartialAggFunction(aggCall: AggregateCall): Seq[SqlAggFunction] = {
    PARTIAL_FINAL_MAP.get(aggCall.getAggregation) match {
      case Some((partialAggFunctions, _)) => partialAggFunctions
      case _ => throw new TableException(
        "Aggregation " + aggCall.getAggregation + " is not supported to split!")
    }
  }

  private def getFinalAggFunction(aggCall: AggregateCall): Seq[SqlAggFunction] = {
    PARTIAL_FINAL_MAP.get(aggCall.getAggregation) match {
      case Some((_, finalAggFunctions)) => finalAggFunctions
      case _ => throw new TableException(
        "Aggregation " + aggCall.getAggregation + " is not supported to split!")
    }
  }

  /**
    * Compute the group sets of the final aggregation.
    *
    * @param groupSet the group set of the previous partial aggregation
    * @param originalGroupSets the group set of the original aggregation
    */
  private def remap(
      groupSet: ImmutableBitSet,
      originalGroupSets: Iterable[ImmutableBitSet]): ImmutableList[ImmutableBitSet] = {
    val builder = ImmutableList.builder[ImmutableBitSet]
    for (originalGroupSet <- originalGroupSets) {
      builder.add(remap(groupSet, originalGroupSet))
    }
    builder.build
  }

  /**
    * Compute the group set of the final aggregation.
    *
    * @param groupSet the group set of the previous partial aggregation
    * @param originalGroupSet the group set of the original aggregation
    */
  private def remap(
      groupSet: ImmutableBitSet,
      originalGroupSet: ImmutableBitSet): ImmutableBitSet = {
    val builder = ImmutableBitSet.builder
    for (bit <- originalGroupSet) {
      builder.set(remap(groupSet, bit))
    }
    builder.build
  }

  private def remap(groupSet: ImmutableBitSet, arg: Int): Int = {
    if (arg < 0) {
      -1
    } else {
      groupSet.indexOf(arg)
    }
  }
}
