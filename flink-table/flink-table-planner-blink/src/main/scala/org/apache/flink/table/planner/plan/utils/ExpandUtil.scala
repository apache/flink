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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.calcite.FlinkRelBuilder
import org.apache.flink.table.planner.plan.nodes.calcite.{Expand, LogicalExpand}

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.ImmutableBitSet

import java.math.BigDecimal
import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

object ExpandUtil {

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
    val groupIdExprs = AggregateUtil.getGroupIdExprIndexes(aggCalls)
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
  private def buildDuplicateFieldMap(
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
    var expandIdFieldName = buildUniqueFieldName(allFieldNames, "$e")
    fieldNameList += expandIdFieldName

    // 3. add duplicate fields
    duplicateFieldIndexes.foreach {
      duplicateFieldIdx =>
        typeList += inputType.getFieldList.get(duplicateFieldIdx).getType
        fieldNameList += buildUniqueFieldName(
          allFieldNames, inputType.getFieldNames.get(duplicateFieldIdx))
    }

    typeFactory.createStructType(typeList, fieldNameList)
  }

  /**
    * Get unique field name based on existed `allFieldNames` collection.
    * NOTES: the new unique field name will be added to existed `allFieldNames` collection.
    */
  private def buildUniqueFieldName(
      allFieldNames: util.Set[String],
      toAddFieldName: String): String = {
    var name: String = toAddFieldName
    var i: Int = 0
    while (allFieldNames.contains(name)) {
      name = toAddFieldName + "_" + i
      i += 1
    }
    allFieldNames.add(name)
    name
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
