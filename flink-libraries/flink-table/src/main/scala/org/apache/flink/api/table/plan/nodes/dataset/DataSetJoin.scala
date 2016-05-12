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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.JoinInfo
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelWriter, BiRel, RelNode}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.operators.join.JoinType
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.FlatJoinRunner
import org.apache.flink.api.table.typeutils.TypeConverter
import org.apache.flink.api.table.{BatchTableEnvironment, TableException}
import org.apache.flink.api.common.functions.FlatJoinFunction
import TypeConverter.determineReturnType
import scala.collection.mutable.ArrayBuffer
import org.apache.calcite.rex.{RexInputRef, RexCall, RexNode}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Flink RelNode which matches along with JoinOperator and its related operations.
  */
class DataSetJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends BiRel(cluster, traitSet, left, right)
  with DataSetRel {

  val translatable = canBeTranslated

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"Join(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString)
      .item("join", joinSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    if (!translatable) {
      // join cannot be translated. Make huge costs
      planner.getCostFactory.makeHugeCost()
    } else {
      // join can be translated. Compute cost estimate
      val children = this.getInputs
      children.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
        val rowCnt = metadata.getRowCount(child)
        val rowSize = this.estimateRowSize(child.getRowType)
        cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
      }
    }

  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val config = tableEnv.getConfig

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw new TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
      )
    }
    else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw new TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: ($joinConditionToString)"
          )
        }
      })
    }

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)

    val generator = new CodeGenerator(config, leftDataSet.getType, Some(rightDataSet.getType))
    val conversion = generator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    var body = ""

    if (joinInfo.isEqui) {
      // only equality condition
      body = s"""
           |${conversion.code}
           |${generator.collectorTerm}.collect(${conversion.resultTerm});
           |""".stripMargin
    }
    else {
      val condition = generator.generateExpression(joinCondition)
      body = s"""
           |${condition.code}
           |if (${condition.resultTerm}) {
           |  ${conversion.code}
           |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
           |}
           |""".stripMargin
    }
    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Any, Any, Any]],
      body,
      returnType)

    val joinFun = new FlatJoinRunner[Any, Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)

    val joinOpName = s"where: ($joinConditionToString), join: ($joinSelectionToString)"

    leftDataSet.join(rightDataSet).where(leftKeys.toArray: _*).equalTo(rightKeys.toArray: _*)
      .`with`(joinFun).name(joinOpName).asInstanceOf[DataSet[Any]]
  }

  private def canBeTranslated: Boolean = {

    val equiCondition =
      joinInfo.getEquiCondition(left, right, cluster.getRexBuilder)

    // joins require at least one equi-condition
    if (equiCondition.isAlwaysTrue) {
      false
    }
    else {
      // check that all equality predicates refer to field refs only (not computed expressions)
      //   Note: Calcite treats equality predicates on expressions as non-equi predicates
      joinCondition match {

        // conjunction of join predicates
        case c: RexCall if c.getOperator.equals(SqlStdOperatorTable.AND) =>

          c.getOperands.asScala
            // look at equality predicates only
            .filter { o =>
            o.isInstanceOf[RexCall] &&
              o.asInstanceOf[RexCall].getOperator.equals(SqlStdOperatorTable.EQUALS)
          }
            // check that both children are field references
            .map { o =>
            o.asInstanceOf[RexCall].getOperands.get(0).isInstanceOf[RexInputRef] &&
              o.asInstanceOf[RexCall].getOperands.get(1).isInstanceOf[RexInputRef]
          }
            // any equality predicate that does not refer to a field reference?
            .reduce( (a, b) => a && b)

        // single equi-join predicate
        case c: RexCall if c.getOperator.equals(SqlStdOperatorTable.EQUALS) =>
          c.getOperands.get(0).isInstanceOf[RexInputRef] &&
            c.getOperands.get(1).isInstanceOf[RexInputRef]
        case _ =>
          false
      }
    }

  }

  private def joinSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {

    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

}
