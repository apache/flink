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

package org.apache.flink.table.plan.rules.datastream

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter, LogicalTableFunctionScan}
import org.apache.calcite.rex.{RexCorrelVariable, RexFieldAccess, RexLocalRef, RexNode}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DataStreamCorrelate
import org.apache.flink.table.plan.nodes.logical.{FlinkLogicalCalc, FlinkLogicalCorrelate, FlinkLogicalTableFunctionScan, FlinkLogicalUncollect}
import org.apache.flink.table.plan.schema.ArrayRelDataType
import org.apache.flink.table.plan.util.ExplodeFunctionUtil

import scala.collection.JavaConversions._

/**
  * Rule to convert a LogicalCorrelate with Uncollect into a DataStreamCorrelate.
  */
class DataStreamCorrelateUnnestRule
  extends ConverterRule(
    classOf[FlinkLogicalCorrelate],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DataStreamCorrelateUnnestRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalCorrelate = call.rel(0).asInstanceOf[FlinkLogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // a filter is pushed above the table function
      case calc: FlinkLogicalCalc =>
        calc.getInput.asInstanceOf[RelSubset]
          .getOriginal.isInstanceOf[Uncollect]
      case unCollect: Uncollect => true
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val join: FlinkLogicalCorrelate = rel.asInstanceOf[FlinkLogicalCorrelate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(join.getInput(0), FlinkConventions.DATASTREAM)
    val right: RelNode = join.getInput(1)

    def convertToCorrelate(relNode: RelNode, condition: Option[RexNode]): DataStreamCorrelate = {
      relNode match {
        case rel: RelSubset =>
          convertToCorrelate(rel.getRelList.get(0), condition)

        case calc: FlinkLogicalCalc =>
          convertToCorrelate(
            calc.getInput.asInstanceOf[RelSubset].getOriginal,
            Some(calc.getProgram.expandLocalRef(calc.getProgram.getCondition)))

        case unCollect: FlinkLogicalUncollect =>
          val arrayRelDataType = unCollect
            .getInput(0)
            .getRowType
            .getFieldList
            .get(0)
            .getValue
            .asInstanceOf[ArrayRelDataType]
          val explodeTableFunc = UserDefinedFunctionUtils.createTableSqlFunctions(
            "explode",
            ExplodeFunctionUtil.explodeTableFuncFromType(arrayRelDataType.typeInfo),
            FlinkTypeFactory.toTypeInfo(arrayRelDataType.getComponentType),
            rel.getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

          val rexCall = rel.getCluster.getRexBuilder.makeCall(
            explodeTableFunc.head,
            rel.getCluster.getRexBuilder.makeFieldAccess(
              extractRexCorrelVariableFromUncollect(unCollect),
              extractRexLocalRefFromUncollect(unCollect).getField.getName,
              false)
            )
          val func = new FlinkLogicalTableFunctionScan(
            rel.getCluster,
            traitSet,
            new util.ArrayList[RelNode](),
            rexCall,
            classOf[Array[Object]],
            new RelRecordType(StructKind.FULLY_QUALIFIED,
              ImmutableList.of(new RelDataTypeFieldImpl(
                "f0",
                0,
                unCollect.getInput(0).getRowType.getFieldList.get(0).getValue.getComponentType))),
            null)
          new DataStreamCorrelate(
            rel.getCluster,
            traitSet,
            convInput,
            func,
            condition,
            rel.getRowType,
            join.getRowType,
            join.getJoinType,
            description)
      }
    }
    convertToCorrelate(right, None)
  }

  private def extractRexCorrelVariableFromUncollect(unCollect: FlinkLogicalUncollect): RexNode = {
    unCollect.getInput(0).asInstanceOf[RelSubset]
      .getRelList.get(0).asInstanceOf[FlinkLogicalCalc]
      .getProgram.getExprList.toList.filter(_ match {
      case _: RexCorrelVariable => true
      case _ => false
    }).head
  }

  private def extractRexLocalRefFromUncollect(unCollect: FlinkLogicalUncollect): RexFieldAccess = {
    unCollect.getInput(0).asInstanceOf[RelSubset]
      .getRelList.get(0).asInstanceOf[FlinkLogicalCalc]
      .getProgram.getExprList.toList.filter(_ match {
      case _: RexFieldAccess => true
      case _ => false
    }).head.asInstanceOf[RexFieldAccess]
  }
}

object DataStreamCorrelateUnnestRule {
  val INSTANCE: RelOptRule = new DataStreamCorrelateUnnestRule
}

