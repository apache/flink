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

import java.util.Collections

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
import org.apache.flink.table.plan.schema.{ArrayRelDataType, MultisetRelDataType}
import org.apache.flink.table.plan.util.ExplodeFunctionUtil

class LogicalUnnestRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  override def matches(call: RelOptRuleCall): Boolean = {

    val join: LogicalCorrelate = call.rel(0).asInstanceOf[LogicalCorrelate]
    val right = join.getRight.asInstanceOf[RelSubset].getOriginal

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        filter.getInput.asInstanceOf[RelSubset].getOriginal match {
          case u: Uncollect => !u.withOrdinality
          case p: LogicalProject => p.getInput.asInstanceOf[RelSubset].getOriginal match {
            case u: Uncollect => !u.withOrdinality
            case _ => false
          }
          case _ => false
        }
      case p: LogicalProject => p.getInput.asInstanceOf[RelSubset].getOriginal match {
        case u: Uncollect => !u.withOrdinality
        case _ => false
      }
      case u: Uncollect => !u.withOrdinality
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate = call.rel(0).asInstanceOf[LogicalCorrelate]

    val outer = correlate.getLeft.asInstanceOf[RelSubset].getOriginal
    val array = correlate.getRight.asInstanceOf[RelSubset].getOriginal

    def convert(relNode: RelNode): RelNode = {
      relNode match {
        case rs: RelSubset =>
          convert(rs.getRelList.get(0))

        case p: LogicalProject =>
          p.copy(
            p.getTraitSet,
            ImmutableList.of(convert(p.getInput.asInstanceOf[RelSubset].getOriginal)))

        case f: LogicalFilter =>
          f.copy(
            f.getTraitSet,
            ImmutableList.of(convert(f.getInput.asInstanceOf[RelSubset].getOriginal)))

        case uc: Uncollect =>
          // convert Uncollect into TableFunctionScan
          val cluster = correlate.getCluster
          val dataType = uc.getInput.getRowType.getFieldList.get(0).getValue
          val (componentType, explodeTableFunc) = dataType match {
            case arrayType: ArrayRelDataType =>
              (arrayType.getComponentType,
                ExplodeFunctionUtil.explodeTableFuncFromType(arrayType.typeInfo))
            case mt: MultisetRelDataType =>
              (mt.getComponentType, ExplodeFunctionUtil.explodeTableFuncFromType(mt.typeInfo))
            case _ => throw new TableException(s"Unsupported UNNEST on type: ${dataType.toString}")
          }

          // create sql function
          val explodeSqlFunc = UserDefinedFunctionUtils.createTableSqlFunction(
            "explode",
            "explode",
            explodeTableFunc,
            FlinkTypeFactory.toTypeInfo(componentType),
            cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

          // create table function call
          val rexCall = cluster.getRexBuilder.makeCall(
            explodeSqlFunc,
            uc.getInput.asInstanceOf[RelSubset]
              .getOriginal.asInstanceOf[LogicalProject].getChildExps
          )

          // determine rel data type of unnest
          val rowType = componentType match {
            case _: AbstractSqlType =>
              new RelRecordType(
                StructKind.FULLY_QUALIFIED,
                ImmutableList.of(new RelDataTypeFieldImpl("f0", 0, componentType)))
            case _: RelRecordType => componentType
            case _ => throw new TableException(
              s"Unsupported component type in UNNEST: ${componentType.toString}")
          }

          // create table function scan
          new LogicalTableFunctionScan(
            cluster,
            correlate.getTraitSet,
            Collections.emptyList(),
            rexCall,
            classOf[Array[Object]],
            rowType,
            null)
      }
    }

    // convert unnest into table function scan
    val tableFunctionScan = convert(array)
    // create correlate with table function scan as input
    val newCorrleate =
      correlate.copy(correlate.getTraitSet, ImmutableList.of(outer, tableFunctionScan))
    call.transformTo(newCorrleate)
  }
}

object LogicalUnnestRule {
  val INSTANCE = new LogicalUnnestRule(
    operand(classOf[LogicalCorrelate], any),
    "LogicalUnnestRule")
}
