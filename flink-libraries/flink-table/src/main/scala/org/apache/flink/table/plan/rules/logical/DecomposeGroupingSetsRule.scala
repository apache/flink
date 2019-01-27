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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.{FlinkRelBuilder, FlinkRelFactories}
import org.apache.flink.table.plan.nodes.calcite.{Expand, LogicalExpand}
import org.apache.flink.table.plan.rules.logical.DecomposeGroupingSetsRule._
import org.apache.flink.table.plan.util.FlinkRelOptUtil

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.ImmutableBitSet

import com.google.common.collect.ImmutableList

import java.math.BigDecimal
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * This rule rewrites an aggregation query with grouping sets into
  * an regular aggregation query with expand.
  *
  * This rule duplicates the input data by two or more times (# number of groupSets +
  * an optional non-distinct group). This will put quite a bit of memory pressure of the used
  * aggregate and exchange operators.
  *
  * This rule will be used for the plan with grouping sets or the plan with distinct aggregations
  * after [[FlinkAggregateExpandDistinctAggregatesRule]]
  * applied.
  *
  * `FlinkAggregateExpandDistinctAggregatesRule` rewrites an aggregate query with
  * distinct aggregations into an expanded double aggregation. The first aggregate has
  * grouping sets in which the regular aggregation expressions and every distinct clause
  * are aggregated in a separate group. The results are then combined in a second aggregate.
  *
  * Examples:
  *
  * MyTable: a: INT, b: BIGINT, c: VARCHAR(32), d: VARCHAR(32)
  *
  * Original records:
  * +-----+-----+-----+-----+
  * |  a  |  b  |  c  |  d  |
  * +-----+-----+-----+-----+
  * |  1  |  1  |  c1 |  d1 |
  * +-----+-----+-----+-----+
  * |  1  |  2  |  c1 |  d2 |
  * +-----+-----+-----+-----+
  * |  2  |  1  |  c1 |  d1 |
  * +-----+-----+-----+-----+
  *
  * Example1 (expand for DISTINCT aggregates):
  *
  * SQL:
  * SELECT a, SUM(DISTINCT b) as t1, COUNT(DISTINCT c) as t2, COUNT(d) as t3 FROM MyTable GROUP BY a
  *
  * Logical plan:
  * {{{
  * LogicalAggregate(group=[{0}], t1=[SUM(DISTINCT $1)], t2=[COUNT(DISTINCT $2)], t3=[COUNT($3)])
  *  LogicalTableScan(table=[[builtin, default, MyTable]])
  * }}}
  *
  * Logical plan after `FlinkAggregateExpandDistinctAggregatesRule` applied:
  * {{{
  * LogicalProject(a=[$0], t1=[$1], t2=[$2], t3=[CAST($3):BIGINT NOT NULL])
  *  LogicalProject(a=[$0], t1=[$1], t2=[$2], $f3=[CASE(IS NOT NULL($3), $3, 0)])
  *   LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
  *     t3=[MIN($3) FILTER $6])
  *    LogicalProject(a=[$0], b=[$1], c=[$2], t3=[$3], $g_1=[=($4, 1)], $g_2=[=($4, 2)],
  *      $g_3=[=($4, 3)])
  *     LogicalAggregate(group=[{0, 1, 2}], groups=[[{0, 1}, {0, 2}, {0}]], t3=[COUNT($3)],
  *       $g=[GROUPING($0, $1, $2)])
  *      LogicalTableScan(table=[[builtin, default, MyTable]])
  * }}}
  *
  * Logical plan after this rule applied:
  * {{{
  * LogicalCalc(expr#0..3=[{inputs}], expr#4=[IS NOT NULL($t3)], ...)
  *  LogicalAggregate(group=[{0}], t1=[SUM($1) FILTER $4], t2=[COUNT($2) FILTER $5],
  *    t3=[MIN($3) FILTER $6])
  *   LogicalCalc(expr#0..4=[{inputs}], ... expr#10=[CASE($t6, $t5, $t8, $t7, $t9)],
  *      expr#11=[1], expr#12=[=($t10, $t11)], ... $g_1=[$t12], ...)
  *    LogicalAggregate(group=[{0, 1, 2, 4}], groups=[[]], t3=[COUNT($3)])
  *     LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], d=[$3], $e=[1]},
  *       {a=[$0], b=[null], c=[$2], d=[$3], $e=[2]}, {a=[$0], b=[null], c=[null], d=[$3], $e=[3]}])
  *      LogicalTableSourceScan(table=[[builtin, default, MyTable]], fields=[a, b, c, d])
  * }}}
  *
  * '$e = 1' is equivalent to 'group by a, b'
  * '$e = 2' is equivalent to 'group by a, c'
  * '$e = 3' is equivalent to 'group by a'
  *
  * Expanded records:
  * +-----+-----+-----+-----+-----+
  * |  a  |  b  |  c  |  d  | $e  |
  * +-----+-----+-----+-----+-----+        ---+---
  * |  1  |  1  | null|  d1 |  1  |           |
  * +-----+-----+-----+-----+-----+           |
  * |  1  | null|  c1 |  d1 |  2  | records expanded by record1
  * +-----+-----+-----+-----+-----+           |
  * |  1  | null| null|  d1 |  3  |           |
  * +-----+-----+-----+-----+-----+        ---+---
  * |  1  |  2  | null|  d2 |  1  |           |
  * +-----+-----+-----+-----+-----+           |
  * |  1  | null|  c1 |  d2 |  2  |  records expanded by record2
  * +-----+-----+-----+-----+-----+           |
  * |  1  | null| null|  d2 |  3  |           |
  * +-----+-----+-----+-----+-----+        ---+---
  * |  2  |  1  | null|  d1 |  1  |           |
  * +-----+-----+-----+-----+-----+           |
  * |  2  | null|  c1 |  d1 |  2  |  records expanded by record3
  * +-----+-----+-----+-----+-----+           |
  * |  2  | null| null|  d1 |  3  |           |
  * +-----+-----+-----+-----+-----+        ---+---
  *
  * Example2 (Some fields are both in DISTINCT aggregates and non-DISTINCT aggregates):
  *
  * SQL:
  * SELECT MAX(a) as t1, COUNT(DISTINCT a) as t2, count(DISTINCT d) as t3 FROM MyTable
  *
  * Field `a` is both in DISTINCT aggregate and `MAX` aggregate,
  * so, `a` should be outputted as two individual fields, one is for `MAX` aggregate,
  * another is for DISTINCT aggregate.
  *
  * Expanded records:
  * +-----+-----+-----+-----+
  * |  a  |  d  | $e  | a_0 |
  * +-----+-----+-----+-----+        ---+---
  * |  1  | null|  1  |  1  |           |
  * +-----+-----+-----+-----+           |
  * | null|  d1 |  2  |  1  |  records expanded by record1
  * +-----+-----+-----+-----+           |
  * | null| null|  3  |  1  |           |
  * +-----+-----+-----+-----+        ---+---
  * |  1  | null|  1  |  1  |           |
  * +-----+-----+-----+-----+           |
  * | null|  d2 |  2  |  1  |  records expanded by record2
  * +-----+-----+-----+-----+           |
  * | null| null|  3  |  1  |           |
  * +-----+-----+-----+-----+        ---+---
  * |  2  | null|  1  |  2  |           |
  * +-----+-----+-----+-----+           |
  * | null|  d1 |  2  |  2  |  records expanded by record3
  * +-----+-----+-----+-----+           |
  * | null| null|  3  |  2  |           |
  * +-----+-----+-----+-----+        ---+---
  *
  * Example3 (expand for CUBE/ROLLUP/GROUPING SETS):
  *
  * SQL:
  * SELECT a, c, SUM(b) as b FROM MyTable GROUP BY GROUPING SETS (a, c)
  *
  * Logical plan:
  * {{{
  * LogicalAggregate(group=[{0, 1}], groups=[[{0}, {1}]], b=[SUM($2)])
  *  LogicalProject(a=[$0], c=[$2], b=[$1])
  *   LogicalTableScan(table=[[builtin, default, MyTable]])
  * }}}
  *
  * Logical plan after this rule applied:
  * {{{
  * LogicalCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}], b=[$t3])
  *  LogicalAggregate(group=[{0, 2, 3}], groups=[[]], b=[SUM($1)])
  *   LogicalExpand(projects=[{a=[$0], b=[$1], c=[null], $e=[1]},
  *     {a=[null], b=[$1], c=[$2], $e=[2]}])
  *    LogicalNativeTableScan(table=[[builtin, default, MyTable]])
  * }}}
  *
  * '$e = 1' is equivalent to 'group by a'
  * '$e = 2' is equivalent to 'group by c'
  *
  * Expanded records:
  * +-----+-----+-----+-----+
  * |  a  |  b  |  c  | $e  |
  * +-----+-----+-----+-----+        ---+---
  * |  1  |  1  | null|  1  |           |
  * +-----+-----+-----+-----+  records expanded by record1
  * | null|  1  |  c1 |  2  |           |
  * +-----+-----+-----+-----+        ---+---
  * |  1  |  2  | null|  1  |           |
  * +-----+-----+-----+-----+  records expanded by record2
  * | null|  2  |  c1 |  2  |           |
  * +-----+-----+-----+-----+        ---+---
  * |  2  |  1  | null|  1  |           |
  * +-----+-----+-----+-----+  records expanded by record3
  * | null|  1  |  c1 |  2  |           |
  * +-----+-----+-----+-----+        ---+---
  *
  * TODO match Aggregate instead of LogicalAggregate
  */
class DecomposeGroupingSetsRule extends RelOptRule(
    operand(classOf[LogicalAggregate], any),
    FlinkRelFactories.FLINK_REL_BUILDER,
    "DecomposeGroupingSetsRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rels(0).asInstanceOf[LogicalAggregate]
    val groupIdExprs = getGroupIdExprIndexes(agg.getAggCallList)
    agg.getGroupSets.size() > 1 || groupIdExprs.nonEmpty
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rels(0).asInstanceOf[LogicalAggregate]
    // Long data type is used to store groupValue in FlinkAggregateExpandDistinctAggregatesRule,
    // and the result of grouping function is a positive value,
    // so the max groupCount must be less than 64.
    if (agg.getGroupCount >= 64) {
      throw new TableException("group count must be less than 64.")
    }

    val aggInput = agg.getInput
    val groupIdExprs = getGroupIdExprIndexes(agg.getAggCallList)
    val aggCallsWithIndexes = agg.getAggCallList.zipWithIndex

    val cluster = agg.getCluster
    val rexBuilder = cluster.getRexBuilder
    val needExpand = agg.getGroupSets.size() > 1

    val relBuilder = call.builder().asInstanceOf[FlinkRelBuilder]
    relBuilder.push(aggInput)

    val (newGroupSet, duplicateFieldMap) = if (needExpand) {
      val (duplicateFieldMap, expandIdIdxInExpand) = buildExpandNode(
        cluster, relBuilder, agg.getAggCallList, agg.getGroupSet, agg.getGroupSets)

      // new groupSet contains original groupSet and expand_id('$e') field
      val newGroupSet = agg.getGroupSet.union(ImmutableBitSet.of(expandIdIdxInExpand))

      (newGroupSet, duplicateFieldMap)
    } else {
      // no need add expand node, only need care about group functions
      (agg.getGroupSet, Map.empty[Integer, Integer])
    }

    val newGroupCount = newGroupSet.cardinality()
    val newAggCalls = aggCallsWithIndexes.collect {
      case (aggCall, idx) if !groupIdExprs.contains(idx) =>
        val newArgList = aggCall.getArgList.map(a => duplicateFieldMap.getOrElse(a, a)).toList
        aggCall.adaptTo(
          relBuilder.peek(), newArgList, aggCall.filterArg, agg.getGroupCount, newGroupCount)
    }

    // create simple aggregate
    relBuilder.aggregate(
      relBuilder.groupKey(newGroupSet, ImmutableList.of[ImmutableBitSet](newGroupSet)),
      newAggCalls)
    val newAgg = relBuilder.peek()

    // create a project to mapping original aggregate's output
    // get names of original grouping fields
    val groupingFieldsName = Seq.range(0, agg.getGroupCount)
        .map(x => agg.getRowType.getFieldNames.get(x))

    // create field access for all original grouping fields
    val groupingFields = agg.getGroupSet.toList.zipWithIndex.map {
      case (_, idx) => rexBuilder.makeInputRef(newAgg, idx)
    }.toArray[RexNode]

    val groupSetsWithIndexes = agg.getGroupSets.zipWithIndex
    // output aggregate calls including `normal` agg call and grouping agg call
    var aggCnt = 0
    val aggFields = aggCallsWithIndexes.map {
      case (aggCall, idx) if groupIdExprs.contains(idx) =>
        if (needExpand) {
          // reference to expand_id('$e') field in new aggregate
          val expandIdIdxInNewAgg = newGroupCount - 1
          val expandIdField = rexBuilder.makeInputRef(newAgg, expandIdIdxInNewAgg)
          // create case when for group expression
          val whenThenElse = groupSetsWithIndexes.flatMap {
            case (subGroupSet, i) =>
              val groupExpr = lowerGroupExpr(rexBuilder, aggCall, groupSetsWithIndexes, i)
              if (i < agg.getGroupSets.size() - 1) {
                //  WHEN/THEN
                val expandIdVal = genExpandId(agg.getGroupSet, subGroupSet)
                val expandIdType = newAgg.getRowType.getFieldList.get(expandIdIdxInNewAgg).getType
                val expandIdLit = rexBuilder.makeLiteral(expandIdVal, expandIdType, false)
                Seq(
                  // when $e = $e_value
                  rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, expandIdField, expandIdLit),
                  // then return group expression literal value
                  groupExpr
                )
              } else {
                // ELSE
                Seq(
                  // else return group expression literal value
                  groupExpr
                )
              }
          }
          rexBuilder.makeCall(SqlStdOperatorTable.CASE, whenThenElse)
        } else {
          // create literal for group expression
          lowerGroupExpr(rexBuilder, aggCall, groupSetsWithIndexes, 0)
        }
      case _ =>
        // create access to aggregation result
        val aggResult = rexBuilder.makeInputRef(newAgg, newGroupCount + aggCnt)
        aggCnt += 1
        aggResult
    }

    // add a projection to establish the result schema and set the values of the group expressions.
    relBuilder.project(
      groupingFields.toSeq ++ aggFields,
      groupingFieldsName ++ agg.getAggCallList.map(_.name))
    relBuilder.convert(agg.getRowType, true)

    call.transformTo(relBuilder.build())
  }
}

object DecomposeGroupingSetsRule {
  val INSTANCE: RelOptRule = new DecomposeGroupingSetsRule

  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex.filter { case (call, _) =>
      call.getAggregation.getKind match {
        case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
        case _ => false
      }
    }.map { case (_, idx) => idx }
  }

  /** Returns a literal for a given group expression. */
  private def lowerGroupExpr(
      builder: RexBuilder,
      call: AggregateCall,
      groupSetsWithIndexes: Seq[(ImmutableBitSet, Int)],
      indexInGroupSets: Int): RexNode = {

    val groupSet = groupSetsWithIndexes(indexInGroupSets)._1
    val groups = groupSet.asSet()
    call.getAggregation.getKind match {
      case SqlKind.GROUP_ID =>
        // https://issues.apache.org/jira/browse/CALCITE-1824
        // GROUP_ID is not in the SQL standard. It is implemented only by Oracle.
        // GROUP_ID is useful only if you have duplicate grouping sets,
        // If grouping sets are distinct, GROUP_ID() will always return zero;
        // Else return the index in the duplicate grouping sets.
        // e.g. SELECT deptno, GROUP_ID() AS g FROM Emp GROUP BY GROUPING SETS (deptno, (), ())
        // As you can see, the grouping set () occurs twice.
        // So there is one row in the result for each occurrence:
        // the first occurrence has g = 0; the second has g = 1.
        val duplicateGroupSetsIndices = groupSetsWithIndexes.filter {
          case (gs, _) => gs.compareTo(groupSet) == 0
        }.map(_._2).toArray[Int]
        require(duplicateGroupSetsIndices.nonEmpty)
        val id: Long = duplicateGroupSetsIndices.indexOf(indexInGroupSets)
        builder.makeLiteral(id, call.getType, false)
      case SqlKind.GROUPING | SqlKind.GROUPING_ID =>
        // GROUPING function is defined in the SQL standard,
        // but the definition of GROUPING is different from in Oracle and in SQL standard:
        // https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions064.htm#SQLRF00647
        //
        // GROUPING_ID function is not defined in the SQL standard, and has the same
        // functionality with GROUPING function in Calcite.
        // our implementation is consistent with Oracle about GROUPING_ID function.
        //
        // NOTES:
        // In Calcite, the java-document of SqlGroupingFunction is not consistent with agg.iq.
        val res: Long = call.getArgList.foldLeft(0L)((res, arg) =>
          (res << 1L) + (if (groups.contains(arg)) 0L else 1L)
        )
        builder.makeLiteral(res, call.getType, false)
      case _ => builder.constantNull()
    }
  }

  /**
    * Build the [[Expand]] node.
    * The input node should be pushed into the RelBuilder before calling this method
    * and the created Expand node will be at the top of the stack of the RelBuilder.
    */
  def buildExpandNode(
      cluster: RelOptCluster,
      relBuilder: FlinkRelBuilder,
      aggCalls: Seq[AggregateCall],
      groupSet: ImmutableBitSet,
      groupSets: ImmutableList[ImmutableBitSet]): (Map[Integer, Integer], Integer) = {
    // find fields which are both in grouping and 'regular' aggCalls (excluding GROUPING aggCalls)
    // e.g.: select max(a) from table group by grouping sets (a, b)
    // field `a` should be outputted as two individual fields,
    // one is for max aggregate, another is for group by.
    //
    // if a 'regular' aggCall's args are all in each sub-groupSet of GroupSets,
    // there is no need output the 'regular' aggCall's args as duplicate fields.
    // e.g. SELECT count(a) as a, count(b) as b, count(c) as c FROM MyTable
    //      GROUP BY GROUPING SETS ((a, b), (a, c))
    // only field 'b' and 'c' need be outputted as duplicate fields.
    val groupIdExprs = getGroupIdExprIndexes(aggCalls)
    val commonGroupSet = groupSets.asList().reduce((g1, g2) => g1.intersect(g2)).asList()
    val duplicateFieldIndexes = aggCalls.zipWithIndex.flatMap {
      case (aggCall, idx) =>
        if (groupIdExprs.contains(idx)) {
          List.empty[Integer]
        } else if (commonGroupSet.containsAll(aggCall.getArgList)) {
          List.empty[Integer]
        } else {
          aggCall.getArgList.diff(commonGroupSet)
        }
    }.intersect(groupSet.asList()).sorted.toArray[Integer]

    val inputType = relBuilder.peek().getRowType

    // expand output fields: original input fields + expand_id field + duplicate fields
    val expandIdIdxInExpand = inputType.getFieldCount
    val duplicateFieldMap = buildDuplicateFieldMap(inputType, duplicateFieldIndexes)

    val expandRowType = buildExpandRowType(
      cluster.getTypeFactory, inputType, duplicateFieldIndexes)
    val expandProjects = createExpandProjects(
      relBuilder.getRexBuilder,
      inputType,
      expandRowType,
      groupSet,
      groupSets,
      duplicateFieldIndexes)

    relBuilder.expand(
      expandRowType, expandProjects, expandIdIdxInExpand)

    (duplicateFieldMap, expandIdIdxInExpand)
  }

  /**
    * Mapping original duplicate field index to new index in [[LogicalExpand]].
    *
    * @param inputType Input row type.
    * @param duplicateFieldIndexes Fields indexes that will be output as duplicate.
    * @return a Map that mapping original index to new index for duplicate fields.
    */
  def buildDuplicateFieldMap(
      inputType: RelDataType,
      duplicateFieldIndexes: Array[Integer]): Map[Integer, Integer] = {
    // original input fields + expand_id field + duplicate fields
    duplicateFieldIndexes.zipWithIndex.map {
      case (duplicateFieldIdx: Integer, idx) =>
        require(duplicateFieldIdx < inputType.getFieldCount)
        val duplicateFieldNewIdx: Integer = inputType.getFieldCount + 1 + idx
        (duplicateFieldIdx, duplicateFieldNewIdx)
    }.toMap[Integer, Integer]
  }

  /**
    * Build row type for [[LogicalExpand]].
    *
    * the order of fields are:
    * first, the input fields,
    * second, expand_id field(to distinguish different expanded rows),
    * last, optional duplicate fields.
    *
    * @param typeFactory Type factory.
    * @param inputType Input row type.
    * @param duplicateFieldIndexes Fields indexes that will be output as duplicate.
    * @return Row type for [[LogicalExpand]].
    */
  def buildExpandRowType(
      typeFactory: RelDataTypeFactory,
      inputType: RelDataType,
      duplicateFieldIndexes: Array[Integer]): RelDataType = {

    // 1. add original input fields
    val typeList = mutable.ListBuffer(inputType.getFieldList.map(_.getType): _*)
    val fieldNameList = mutable.ListBuffer(inputType.getFieldNames: _*)
    val allFieldNames = mutable.Set[String](fieldNameList: _*)

    // 2. add expand_id('$e') field
    typeList += typeFactory.createTypeWithNullability(
      typeFactory.createSqlType(SqlTypeName.BIGINT), false)
    var expandIdFieldName = FlinkRelOptUtil.buildUniqueFieldName(allFieldNames, "$e")
    fieldNameList += expandIdFieldName

    // 3. add duplicate fields
    duplicateFieldIndexes.foreach {
      duplicateFieldIdx =>
        typeList += inputType.getFieldList.get(duplicateFieldIdx).getType
        fieldNameList += FlinkRelOptUtil.buildUniqueFieldName(
          allFieldNames, inputType.getFieldNames.get(duplicateFieldIdx))
    }

    typeFactory.createStructType(typeList, fieldNameList)
  }

  /**
    * Create Project list for [[LogicalExpand]].
    * One input row will expand to multiple output rows, so multi projects will be created.
    *
    * @param rexBuilder Rex builder.
    * @param inputType Input row type.
    * @param outputType Row type of [[LogicalExpand]].
    * @param groupSet The original groupSet of a aggregate before expanded.
    * @param groupSets The original groupSets of a aggregate before expanded.
    * @param duplicateFieldIndexes Fields indexes that will be output as duplicate.
    * @return List of expressions of expanded row.
    */
  def createExpandProjects(
      rexBuilder: RexBuilder,
      inputType: RelDataType,
      outputType: RelDataType,
      groupSet: ImmutableBitSet,
      groupSets: ImmutableList[ImmutableBitSet],
      duplicateFieldIndexes: Array[Integer]): util.List[util.List[RexNode]] = {

    val fullGroupList = groupSet.toArray
    require(!groupSets.isEmpty && fullGroupList.nonEmpty)
    val fieldCount = inputType.getFieldCount + 1 + duplicateFieldIndexes.length
    require(fieldCount == outputType.getFieldCount)

    // expand for each groupSet
    val expandProjects = groupSets.map { subGroupSet =>
      val subGroup = subGroupSet.toArray
      val projects: util.List[RexNode] = new util.ArrayList[RexNode]()

      // output the input fields
      for (i <- 0 until inputType.getFieldCount) {
        val shouldOutputValue = subGroup.contains(i) || !fullGroupList.contains(i)
        val resultType = inputType.getFieldList.get(i).getType
        val project = if (shouldOutputValue) {
          rexBuilder.makeInputRef(resultType, i)
        } else {
          rexBuilder.makeNullLiteral(resultType)
        }
        projects.add(project)
      }

      // output for expand_id('$e') field
      val expandId = genExpandId(groupSet, subGroupSet)
      val expandIdField = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(expandId))
      projects.add(expandIdField)

      // TODO only need output duplicate fields for the row against 'regular' aggregates
      // currently, we can't distinguish that
      // an expand row is for 'regular' aggregates or for 'distinct' aggregates
      duplicateFieldIndexes.foreach {
        duplicateFieldIdx =>
          val resultType = inputType.getFieldList.get(duplicateFieldIdx).getType
          val duplicateField = rexBuilder.makeInputRef(resultType, duplicateFieldIdx)
          projects.add(duplicateField)
      }

      projects
    }
    expandProjects
  }

  /**
    * generate expand_id('$e' field) value to distinguish different expanded rows.
    */
  def genExpandId(fullGroupSet: ImmutableBitSet, groupSet: ImmutableBitSet): Long = {
    var v: Long = 0L
    var x: Long = 1L << (fullGroupSet.cardinality - 1)
    assert(fullGroupSet.contains(groupSet))
    for (i <- fullGroupSet) {
      if (!groupSet.get(i)) v |= x
      x >>= 1
    }
    v
  }
}
