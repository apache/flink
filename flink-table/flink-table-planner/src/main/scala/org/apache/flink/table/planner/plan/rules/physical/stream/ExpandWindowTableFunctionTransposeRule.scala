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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.functions.sql.SqlWindowTableFunction
import org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalExpand, StreamPhysicalWindowAggregate, StreamPhysicalWindowTableFunction}
import org.apache.flink.table.planner.plan.utils.WindowUtil
import org.apache.flink.table.planner.plan.utils.WindowUtil.buildNewProgramWithoutWindowColumns

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode, RexProgram}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * This rule transposes [[StreamPhysicalExpand]] past [[StreamPhysicalWindowTableFunction]] to make
 * [[PullUpWindowTableFunctionIntoWindowAggregateRule]] can match the rel tree pattern and optimize
 * them into [[StreamPhysicalWindowAggregate]].
 *
 * Example:
 *
 * MyTable: a INT, c STRING, rowtime TIMESTAMP(3)
 *
 * SQL:
 * {{{
 * SELECT
 *    window_start,
 *    window_end,
 *    count(distinct a),
 *    count(distinct c)
 * FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
 * GROUP BY window_start, window_end
 * }}}
 *
 * We will get part of the initial physical plan like following:
 * {{{
 * WindowAggregate(groupBy=[$f4, $f5], window=[TUMBLE(win_start=[window_start],
 * win_end=[window_end], size=[15 min])], select=[$f4, $f5, COUNT(DISTINCT a) FILTER $g_1 AS $f2,
 * COUNT(DISTINCT c) FILTER $g_2 AS $f3, start('w$) AS window_start, end('w$) AS window_end])
 * +- Exchange(distribution=[hash[$f4, $f5]])
 *    +- Calc(select=[window_start, window_end, a, c, $f4, $f5, =($e, 1) AS $g_1, =($e, 2) AS $g_2])
 *       +- Expand(projects=[{window_start, window_end, a, c, $f4, null AS $f5, 1 AS $e},
 *       {window_start, window_end, a, c, null AS $f4, $f5, 2 AS $e}])
 *          +- Calc(select=[window_start, window_end, a, c,
 *          MOD(HASH_CODE(a), 1024) AS $f4, MOD(HASH_CODE(c), 1024) AS $f5])
 *             +- WindowTableFunction(window=[TUMBLE(time_col=[rowtime], size=[15 min])])
 * }}}
 *
 * However, it can't match [[PullUpWindowTableFunctionIntoWindowAggregateRule]], because
 * [[StreamPhysicalWindowTableFunction]] is not near [[StreamPhysicalWindowAggregate]].
 * So we need to transpose [[StreamPhysicalExpand]] past [[StreamPhysicalWindowTableFunction]]
 * to make the part of rel tree like this which can be matched by
 * [[PullUpWindowTableFunctionIntoWindowAggregateRule]].
 *
 * {{{
 * WindowAggregate(groupBy=[$f4, $f5], window=[TUMBLE(win_start=[window_start],
 * win_end=[window_end], size=[15 min])], select=[$f4, $f5, COUNT(DISTINCT a) FILTER $g_1 AS $f2,
 * COUNT(DISTINCT c) FILTER $g_2 AS $f3, start('w$) AS window_start, end('w$) AS window_end])
 * +- Exchange(distribution=[hash[$f4, $f5]])
 *   +- Calc(select=[window_start, window_end, a, c, $f4, $f5, ($e = 1) AS $g_1, ($e = 2) AS $g_2])
 *     +- WindowTableFunction(window=[TUMBLE(time_col=[rowtime], size=[15 min])])
 *       +- Expand(...)
 * }}}
 * </pre>
 */
class ExpandWindowTableFunctionTransposeRule
  extends RelOptRule(
    operand(classOf[StreamPhysicalExpand],
      operand(classOf[StreamPhysicalCalc],
        operand(classOf[StreamPhysicalWindowTableFunction], any()))),
    "ExpandWindowTableFunctionTransposeRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val expand: StreamPhysicalExpand = call.rel(0)
    val calc: StreamPhysicalCalc = call.rel(1)
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(calc.getCluster.getMetadataQuery)

    // condition and projection of Calc shouldn't contain calls on window columns,
    // otherwise, we can't transpose WindowTVF and Calc
    if (WindowUtil.calcContainsCallsOnWindowColumns(calc, fmq)) {
      return false
    }

    // we only transpose WindowTVF when expand propagate window_start and window_end,
    // otherwise, it's meaningless to transpose
    val expandWindowProps = fmq.getRelWindowProperties(expand)
    expandWindowProps != null &&
      !expandWindowProps.getWindowStartColumns.isEmpty &&
      !expandWindowProps.getWindowEndColumns.isEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val expand: StreamPhysicalExpand = call.rel(0)
    val calc: StreamPhysicalCalc = call.rel(1)
    val windowTVF: StreamPhysicalWindowTableFunction = call.rel(2)
    val cluster = expand.getCluster
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(cluster.getMetadataQuery)
    val typeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val input = windowTVF.getInput
    val inputRowType = input.getRowType

    val requiredInputTraitSet = input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(input, requiredInputTraitSet)

    // -------------------------------------------------------------------------
    //  1. transpose Calc and WindowTVF, build the new Calc node (the top node)
    // -------------------------------------------------------------------------
    val windowColumns = fmq.getRelWindowProperties(windowTVF).getWindowColumns
    val (newProgram, fieldShifting, newTimeField, timeFieldAdded) =
      buildNewProgramWithoutWindowColumns(
        cluster.getRexBuilder,
        calc.getProgram,
        inputRowType,
        windowTVF.windowing.getTimeAttributeIndex,
        windowColumns.toArray)
    val newCalc = new StreamPhysicalCalc(
      cluster,
      calc.getTraitSet,
      newInput,
      newProgram,
      newProgram.getOutputRowType)

    // -------------------------------------------------------------------------
    //  2. Adjust input ref index in Expand, append time attribute ref if needed
    // -------------------------------------------------------------------------
    val newExpand = buildNewExpand(expand, newCalc, fieldShifting, newTimeField, timeFieldAdded)

    // -------------------------------------------------------------------------
    //  3. Apply WindowTVF on the new Expand node
    // -------------------------------------------------------------------------
    val newOutputType = SqlWindowTableFunction.inferRowType(
      typeFactory,
      newExpand.getRowType,
      typeFactory.createFieldTypeFromLogicalType(windowTVF.windowing.getTimeAttributeType))
    val timeAttributeOnExpand = if (timeFieldAdded) {
      // the time attribute ref is appended
      newExpand.getRowType.getFieldCount - 1
    } else {
      newTimeField
    }
    val newWindowing = new TimeAttributeWindowingStrategy(
      windowTVF.windowing.getWindow,
      windowTVF.windowing.getTimeAttributeType,
      timeAttributeOnExpand)
    val newWindowTVF = new StreamPhysicalWindowTableFunction(
      cluster,
      windowTVF.getTraitSet,
      newExpand,
      newOutputType,
      newWindowing,
      windowTVF.emitPerRecord)

    // -------------------------------------------------------------------------
    //  4. Apply Calc on the new WindowTVF to adjust the fields mapping
    // -------------------------------------------------------------------------
    val projectionMapping = getProjectionMapping(fmq, expand, newWindowTVF)
    val projectExprs = projectionMapping.map(RexInputRef.of(_, newWindowTVF.getRowType))
    val topRexProgram = RexProgram.create(
      newWindowTVF.getRowType,
      projectExprs.toList.asJava,
      null, // no filter
      expand.getRowType,
      cluster.getRexBuilder)
    val topCalc = new StreamPhysicalCalc(
      cluster,
      expand.getTraitSet,
      newWindowTVF,
      topRexProgram,
      topRexProgram.getOutputRowType)

    // -------------------------------------------------------------------------
    //  5. Finish
    // -------------------------------------------------------------------------
    call.transformTo(topCalc)
  }

  private def buildNewExpand(
      expand: StreamPhysicalExpand,
      newCalc: StreamPhysicalCalc,
      inputFieldShifting: Array[Int],
      newTimeField: Int,
      timeFieldAdded: Boolean): StreamPhysicalExpand = {
    val newInputRowType = newCalc.getRowType
    val expandIdIndex = expand.expandIdIndex
    var newExpandIdIndex = -1
    val newProjects = expand.projects.asScala.map { exprs =>
      val newExprs = ArrayBuffer[RexNode]()
      var baseOffset = 0
      exprs.asScala.zipWithIndex.foreach {
        case (ref: RexInputRef, _) if inputFieldShifting(ref.getIndex) < 0 =>
          // skip the window columns
        case (ref: RexInputRef, _) =>
          val newInputIndex = inputFieldShifting(ref.getIndex)
          newExprs += RexInputRef.of(newInputIndex, newInputRowType)
          // we only use the type from input ref instead of literal
          baseOffset += 1
        case (lit: RexLiteral, exprIndex) =>
          newExprs += lit
          if (exprIndex == expandIdIndex) {
            // this is the expand id, we should remember the new index of expand id
            // and update type for this expr
            newExpandIdIndex = baseOffset
          }
          baseOffset += 1
        case exp@_ =>
          throw new IllegalArgumentException(
            "Expand node should only contain RexInputRef and RexLiteral, but got " + exp)
      }
      if (timeFieldAdded) {
        // append time attribute reference if needed
        newExprs += RexInputRef.of(newTimeField, newInputRowType)
      }
      newExprs.asJava
    }

    new StreamPhysicalExpand(
      expand.getCluster,
      expand.getTraitSet,
      newCalc,
      newProjects.asJava,
      newExpandIdIndex
    )
  }

  private def getProjectionMapping(
      fmq: FlinkRelMetadataQuery,
      oldExpand: StreamPhysicalExpand,
      newWindowTVF: StreamPhysicalWindowTableFunction): Array[Int] = {
    val windowProps = fmq.getRelWindowProperties(oldExpand)
    val startColumns = windowProps.getWindowStartColumns.toArray
    val endColumns = windowProps.getWindowEndColumns.toArray
    val timeColumns = windowProps.getWindowTimeColumns.toArray
    val newWindowTimePos = newWindowTVF.getRowType.getFieldCount - 1
    val newWindowEndPos = newWindowTVF.getRowType.getFieldCount - 2
    val newWindowStartPos = newWindowTVF.getRowType.getFieldCount - 3
    var numWindowColumns = 0
    val projectMapping = ArrayBuffer[Int]()
    (0 until oldExpand.getRowType.getFieldCount).foreach { index =>
      if (startColumns.contains(index)) {
        projectMapping += newWindowStartPos
        numWindowColumns += 1
      } else if (endColumns.contains(index)) {
        projectMapping += newWindowEndPos
        numWindowColumns += 1
      } else if (timeColumns.contains(index)) {
        projectMapping += newWindowTimePos
        numWindowColumns += 1
      } else {
        projectMapping += index - numWindowColumns
      }
    }
    projectMapping.toArray
  }
}

object ExpandWindowTableFunctionTransposeRule {
  val INSTANCE = new ExpandWindowTableFunctionTransposeRule
}
