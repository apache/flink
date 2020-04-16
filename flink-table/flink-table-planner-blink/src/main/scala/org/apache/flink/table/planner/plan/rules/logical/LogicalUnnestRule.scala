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

import org.apache.flink.api.java.typeutils.MapTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.FunctionIdentifier
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils
import org.apache.flink.table.planner.plan.utils.ExplodeFunctionUtil
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.logical.RowType
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataTypeFieldImpl, RelRecordType, StructKind}
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.logical._
import org.apache.calcite.sql.`type`.{AbstractSqlType, ArraySqlType, MapSqlType, MultisetSqlType}
import java.util.Collections

import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory

/**
  * Planner rule that rewrites UNNEST to explode function.
  *
  * Note: This class can only be used in HepPlanner.
  */
class LogicalUnnestRule(
    operand: RelOptRuleOperand,
    description: String)
  extends RelOptRule(operand, description) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: LogicalCorrelate = call.rel(0)
    val right = getRel(join.getRight)

    right match {
      // a filter is pushed above the table function
      case filter: LogicalFilter =>
        getRel(filter.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case p: LogicalProject => getRel(p.getInput) match {
            case u: Uncollect => !u.withOrdinality
            case _ => false
          }
          case _ => false
        }
      case project: LogicalProject =>
        getRel(project.getInput) match {
          case u: Uncollect => !u.withOrdinality
          case _ => false
        }
      case u: Uncollect => !u.withOrdinality
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val correlate: LogicalCorrelate = call.rel(0)
    val outer = getRel(correlate.getLeft)
    val array = getRel(correlate.getRight)

    def convert(relNode: RelNode): RelNode = {
      relNode match {
        case rs: HepRelVertex =>
          convert(getRel(rs))

        case f: LogicalProject =>
          f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput))))

        case f: LogicalFilter =>
          f.copy(f.getTraitSet, ImmutableList.of(convert(getRel(f.getInput))))

        case uc: Uncollect =>
          // convert Uncollect into TableFunctionScan
          val cluster = correlate.getCluster
          val dataType = uc.getInput.getRowType.getFieldList.get(0).getValue
          val (componentType, explodeTableFunc) = dataType match {
            case arrayType: ArraySqlType =>
              (arrayType.getComponentType,
                ExplodeFunctionUtil.explodeTableFuncFromType(
                  fromLogicalTypeToTypeInfo(toLogicalType(arrayType))))

            case map: MapSqlType =>
              val keyType = toLogicalType(map.getKeyType)
              val valueType = toLogicalType(map.getValueType)
              val rowInternalType = RowType.of(keyType, valueType)
              val componentType = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
                  .createFieldTypeFromLogicalType(rowInternalType)
              val mapTypeInfo = new MapTypeInfo(
                fromLogicalTypeToTypeInfo(keyType),
                fromLogicalTypeToTypeInfo(valueType)
              )
              val explodeFunction = ExplodeFunctionUtil.explodeTableFuncFromType(mapTypeInfo)
              (componentType, explodeFunction)

            case mt: MultisetSqlType =>
              (mt.getComponentType,
                ExplodeFunctionUtil.explodeTableFuncFromType(
                  fromLogicalTypeToTypeInfo(toLogicalType(mt))))
            case _ => throw new TableException(s"Unsupported UNNEST on type: ${dataType.toString}")
          }

          // create sql function
          val explodeSqlFunc = UserDefinedFunctionUtils.createTableSqlFunction(
            FunctionIdentifier.of("explode"),
            "explode",
            explodeTableFunc,
            fromLogicalTypeToDataType(toLogicalType(componentType)),
            cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])

          // create table function call
          // TODO use BridgingSqlFunction once we remove TableSqlFunction
          val rexCall = cluster.getRexBuilder.makeCall(
            explodeSqlFunc.getRowType(unwrapTypeFactory(cluster), Collections.emptyList()),
            explodeSqlFunc,
            getRel(uc.getInput).asInstanceOf[LogicalProject].getChildExps
          )

          // determine rel data type of unnest
          val rowType = componentType match {
            case _: AbstractSqlType =>
              new RelRecordType(
                StructKind.FULLY_QUALIFIED,
                ImmutableList.of(new RelDataTypeFieldImpl("f0", 0, componentType)))
            case _: RelRecordType => componentType
            case _ => throw new TableException(
              s"Unsupported multiset component type in UNNEST: ${componentType.toString}")
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
    val newCorrelate =
      correlate.copy(correlate.getTraitSet, ImmutableList.of(outer, tableFunctionScan))
    call.transformTo(newCorrelate)
  }

  private def getRel(rel: RelNode): RelNode = {
    rel match {
      case vertex: HepRelVertex => vertex.getCurrentRel
      case _ => rel
    }
  }
}

object LogicalUnnestRule {
  val INSTANCE = new LogicalUnnestRule(
    operand(classOf[LogicalCorrelate], any),
    "LogicalUnnestRule")
}
