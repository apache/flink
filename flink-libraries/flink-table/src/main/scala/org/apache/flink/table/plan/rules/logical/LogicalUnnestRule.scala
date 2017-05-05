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

import java.util

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.`type`.{RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._
import org.apache.calcite.sql.`type`.AbstractSqlType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.plan.schema.ArrayRelDataType
import org.apache.flink.table.plan.util.ExplodeFunctionUtil

/**
  * Created by suez on 5/4/17.
  */
class LogicalUnnestRule(operand: RelOptRuleOperand,
                        description: String)
  extends RelOptRule(operand, description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalCorrelate = call.rel(0).asInstanceOf[LogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        filter.getInput.asInstanceOf[RelSubset]
          .getOriginal.isInstanceOf[Uncollect]
      case unCollect: Uncollect => true
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalCorrelate]
    transformUnnestToCorrelate(oldRel, call)
  }

  private def transformUnnestToCorrelate(correlate: LogicalCorrelate,
                                         call: RelOptRuleCall) = {
    val logicalTableScan =
      correlate.getLeft.asInstanceOf[RelSubset].getOriginal.asInstanceOf[LogicalTableScan]
    val right =
      correlate.getRight.asInstanceOf[RelSubset].getOriginal
    def convert(relNode: RelNode): RelNode = {
      relNode match {
        case rel: RelSubset =>
          convert(rel.getRelList.get(0))

        case filter: LogicalFilter =>
          filter.copy(filter.getTraitSet, ImmutableList.of(convert(
            filter.getInput.asInstanceOf[RelSubset].getOriginal)))

        case unCollect: Uncollect =>
          val cluster = correlate.getCluster
          val traitSet = correlate.getTraitSet
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
            cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

          val rexCall = cluster.getRexBuilder.makeCall(
            explodeTableFunc.head,
            unCollect.getInput(0).asInstanceOf[RelSubset].getRelList.get(0).getChildExps
          )
          val componentType =
            unCollect.getInput(0).getRowType.getFieldList.get(0).getValue.getComponentType
          val rowType = unCollect.getInput(0)
            .getRowType.getFieldList.get(0).getValue.getComponentType match {
            case _: AbstractSqlType => new RelRecordType(StructKind.FULLY_QUALIFIED,
              ImmutableList.of(new RelDataTypeFieldImpl("f0", 0, componentType)))
            case _: RelRecordType => componentType
            case _ => throw TableException(s"Unsupported row type: ${componentType.toString}")
          }
          new LogicalTableFunctionScan(
            cluster,
            traitSet,
            new util.ArrayList[RelNode](),
            rexCall,
            classOf[Array[Object]],
            rowType,
            null)
      }
    }
    val logicalTableFunctionScan = convert(right)
    val newRel = correlate.copy(correlate.getTraitSet, ImmutableList.of(logicalTableScan, logicalTableFunctionScan))
    call.transformTo(newRel)
  }
}

object LogicalUnnestRule {
  val INSTANCE = new LogicalUnnestRule(
    operand(classOf[LogicalCorrelate], any),
    "LogicalUnnestRule")
}
