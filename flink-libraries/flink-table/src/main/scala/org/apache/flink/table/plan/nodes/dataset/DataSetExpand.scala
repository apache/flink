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

package org.apache.flink.table.plan.nodes.dataset

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.{CodeGenUtils, CodeGenerator}
import org.apache.flink.table.runtime.FlatMapRunner
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Flink RelNode to expand records base on GROUPING SETS.
  *
  * For Example:
  *
  * MyTable:
  * a: INT, b: BIGINT, c: VARCHAR(32)
  *
  * Origin records:
  * +-----+-----+-----+
  * |  a  |  b  |  c  |
  * +-----+-----+-----+
  * |  1  |  1  |  c1 |
  * +-----+-----+-----+
  * |  1  |  2  |  c1 |
  * +-----+-----+-----+
  * |  2  |  1  |  c1 |
  * +-----+-----+-----+
  *
  * SQL:
  * SELECT a, c, SUM(b) as b FROM MyTable GROUP BY GROUPING SETS (a, c)
  *
  * please refer to the doc of [[org.apache.flink.table.plan.rules.dataSet.DataSetAggregateRule]] to
  * see the plan of above SQL.
  *
  * After expanded data:
  * +-----+-----+-----+-----+-----+
  * |  a  |  b  |  c  | i$a | i$c |
  * +-----+-----+-----+-----+-----+  ---+---
  * |  1  |  1  | null|false| true|     |
  * +-----+-----+-----+-----+-----+ records expanded by first record
  * | null|  1  |  c1 |true |false|     |
  * +-----+-----+-----+-----+-----+  ---+---
  * |  1  |  2  | null|false| true|     |
  * +-----+-----+-----+-----+-----+ records expanded by second record
  * | null|  2  |  c1 | true|false|     |
  * +-----+-----+-----+-----+-----+  ---+---
  * |  2  |  1  | null|false| true|     |
  * +-----+-----+-----+-----+-----+ records expanded by third record
  * | null|  1  |  c1 | true|false|     |
  * +-----+-----+-----+-----+-----+  ---+---
  */
class DataSetExpand(
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  input: RelNode,
  groupSets: ImmutableList[ImmutableBitSet],
  aggRelDataType: RelDataType,
  ruleDescription: String)
  extends SingleRel(cluster, traitSet, input)
    with DataSetRel {

  Preconditions.checkArgument(!groupSets.isEmpty)
  val fullGroupList = ImmutableBitSet.union(groupSets).toArray
  Preconditions.checkArgument(fullGroupList.nonEmpty)

  override def deriveRowType(): RelDataType = {
    val typeFactory = getCluster.getTypeFactory
    val inputRowType = getInput().getRowType

    val typeList = mutable.ListBuffer(inputRowType.getFieldList.asScala.map(_.getType): _*)
    val fieldNameList = mutable.ListBuffer(inputRowType.getFieldNames.asScala: _*)

    // get field type and field name from agg row type base on Aggregate.java#deriveRowType
    for (i <- 0 until fullGroupList.size) {
      val index = i + fullGroupList.size
      typeList += aggRelDataType.getFieldList.get(index).getType
      fieldNameList += aggRelDataType.getFieldNames.get(index)
    }

    typeFactory.createStructType(typeList.asJava, fieldNameList.asJava)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetExpand(
      cluster,
      traitSet,
      inputs.get(0),
      groupSets,
      aggRelDataType,
      ruleDescription
    )
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val child = this.getInput
    val rowCnt = metadata.getRowCount(child)
    val rowSize = estimateRowSize(getRowType)

    planner.getCostFactory.makeCost(rowCnt * fullGroupList.length, rowCnt * rowSize, 0)
  }

  override def toString: String = {
    s"Expand(expand: (${rowTypeToString(getRowType)}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item("expand", rowTypeToString(getRowType))
  }

  private def rowTypeToString(rowType: RelDataType): String = {
    rowType.getFieldList.asScala.map(_.getName).mkString(", ")
  }

  override def translateToPlan(tableEnv: BatchTableEnvironment): DataSet[Row] = {
    val config = tableEnv.getConfig

    val inputDS = getInput.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(getRowType)
    val inputType = inputDS.getType
    val inputRowArity = inputType.getArity

    val generator = new CodeGenerator(config, false, inputType)
    val expandCode = mutable.ArrayBuffer[String]("\n")

    groupSets.asScala.foreach { subGroupSet =>
      val group = subGroupSet.toArray

      val rowField = CodeGenUtils.newName("expandRow")
      expandCode += s"org.apache.flink.types.Row $rowField " +
        s"= new org.apache.flink.types.Row(${inputRowArity + fullGroupList.length});"

      for (i <- 0 until inputRowArity) {
        if (group.contains(i) || !fullGroupList.contains(i)) {
          val expr = generator.generateInputAccess(inputType, generator.input1Term, i, None)
          expandCode += s"$rowField.setField($i, ${expr.resultTerm});"
        } else {
          expandCode += s"$rowField.setField($i, null);"
        }
      }

      for (i <- 0 until fullGroupList.length) {
        val fieldIndex = i + inputRowArity
        val isNull = !group.contains(fullGroupList(i))
        expandCode += s"$rowField.setField($fieldIndex, $isNull);"
      }

      expandCode += s"${generator.collectorTerm}.collect($rowField);"
      expandCode += "\n"
    }

    val body =
      s"""
        ${expandCode.mkString("\n")}
       """.stripMargin

    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatMapFunction[Row, Row]],
      body,
      returnType)

    val mapFunc = new FlatMapRunner[Row, Row](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)
    inputDS.flatMap(mapFunc).name(toString)
  }
}
