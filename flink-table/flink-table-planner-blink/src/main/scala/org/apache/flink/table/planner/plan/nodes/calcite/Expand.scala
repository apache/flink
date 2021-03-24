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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.api.DataTypes.NULL
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.utils.{ExpandUtil, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.{RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.util.Litmus

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Relational expression that apply a number of projects to every input row,
  * hence we will get multiple output rows for an input row.
  *
  * <p/> Values of expand_id should be unique.
  *
  * @param cluster       cluster that this relational expression belongs to
  * @param traits        the traits of this rel
  * @param input         input relational expression
  * @param projects      all projects, each project contains list of expressions for
  *                      the output columns
  * @param expandIdIndex expand_id('$e') field index
  */
abstract class Expand(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    input: RelNode,
    val projects: util.List[util.List[RexNode]],
    val expandIdIndex: Int)
  extends SingleRel(cluster, traits, input) {

  isValid(Litmus.THROW, null)

  override def isValid(litmus: Litmus, context: RelNode.Context): Boolean = {
    if (projects.size() <= 1) {
      return litmus.fail("Expand should output more than one rows, otherwise use Project.")
    }
    val fieldLen = projects.get(0).size()
    if (projects.exists(_.size != fieldLen)) {
      return litmus.fail("all projects' field count should be equal.")
    }

    // do type check and derived row type info will be cached by framework
    try {
      deriveRowType()
    } catch {
      case exp: TableException =>
        return litmus.fail(exp.getMessage)
    }

    if (expandIdIndex < 0 || expandIdIndex >= fieldLen) {
      return litmus.fail(
        "expand_id field index should be greater than 0 and less than output field count.")
    }
    val expandIdValues = new util.HashSet[Any]()
    for (project <- projects) {
      project.get(expandIdIndex) match {
        case literal: RexLiteral => expandIdValues.add(literal.getValue)
        case _ => return litmus.fail("expand_id value should not be null.")
      }
    }
    if (expandIdValues.size() != projects.size()) {
      return litmus.fail("values of expand_id should be unique.")
    }
    litmus.succeed()
  }

  override def deriveRowType(): RelDataType = {
    val inputNames = input.getRowType.getFieldNames
    val fieldNameSet = mutable.Set[String](inputNames: _*)
    val rowTypes = mutable.ListBuffer[RelDataType]()
    val outputNames = mutable.ListBuffer[String]()
    val fieldLen = projects.get(0).size()
    val inputNameRefCnt = mutable.Map[String, Int]()

    for (fieldIndex <- 0 until fieldLen) {
      val fieldTypes = mutable.ListBuffer[RelDataType]()
      val fieldNames = mutable.ListBuffer[String]()
      for (projectIndex <- 0 until projects.size()) {
        val rexNode = projects.get(projectIndex).get(fieldIndex)
        fieldTypes += rexNode.getType
        rexNode match {
          case ref: RexInputRef =>
            fieldNames += inputNames.get(ref.getIndex)
          case _: RexLiteral => // ignore
          case exp@_ =>
            throw new TableException(
              "Expand node only support RexInputRef and RexLiteral, but got " + exp)
        }
      }
      if (!fieldNames.isEmpty) {
        val inputName = fieldNames(0)
        val refCnt = inputNameRefCnt.getOrElse(inputName, 0) + 1
        inputNameRefCnt.put(inputName, refCnt)
        outputNames += ExpandUtil.buildDuplicateFieldName(
          fieldNameSet,
          inputName,
          inputNameRefCnt.get(inputName).get)
      } else if (fieldIndex == expandIdIndex) {
        outputNames += ExpandUtil.buildUniqueFieldName(fieldNameSet, "$e")
      } else {
        outputNames += ExpandUtil.buildUniqueFieldName(fieldNameSet, "$f" + fieldIndex)
      }

      val leastRestrictive = input.getCluster.getTypeFactory.leastRestrictive(fieldTypes)
      // if leastRestrictive type is null or type name is NULL means we can not support given
      // projects with different column types (NULL type name is reserved for untyped literals only)
      if (leastRestrictive == null || leastRestrictive.getSqlTypeName == NULL) {
        throw new TableException(
          "Expand node only support projects that have common types, but got a column with " +
            "different types which can not derive a least restrictive common type: column index[" +
            fieldIndex + "], column types[" + fieldTypes.mkString(",") + "]")
      } else {
        rowTypes += leastRestrictive
      }
    }
    cluster.getTypeFactory.createStructType(rowTypes, outputNames)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("projects", RelExplainUtil.projectsToString(projects, input.getRowType, getRowType))
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this.getInput) * projects.size()
    planner.getCostFactory.makeCost(rowCnt, rowCnt, 0)
  }

  override def estimateRowCount(mq: RelMetadataQuery): Double = {
    val childRowCnt = mq.getRowCount(this.getInput)
    if (childRowCnt != null) {
      childRowCnt * projects.size()
    } else {
      null.asInstanceOf[Double]
    }
  }
}
