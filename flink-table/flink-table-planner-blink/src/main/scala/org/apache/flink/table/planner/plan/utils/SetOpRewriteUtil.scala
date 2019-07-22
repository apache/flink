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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalRowType
import org.apache.flink.table.planner.functions.tablefunctions.ReplicateRows
import org.apache.flink.table.planner.functions.utils.{TableSqlFunction, UserDefinedFunctionUtils}
import org.apache.flink.table.planner.plan.schema.TypedFlinkTableFunction
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinRelType, SetOp}
import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util

import java.util

import scala.collection.JavaConversions._

/**
  * Util class that rewrite [[SetOp]].
  */
object SetOpRewriteUtil {

  /**
    * Generate equals condition by keys (The index on both sides is the same) to
    * join left relNode and right relNode.
    */
  def generateEqualsCondition(
      relBuilder: RelBuilder,
      left: RelNode,
      right: RelNode,
      keys: Seq[Int]): Seq[RexNode] = {
    val rexBuilder = relBuilder.getRexBuilder
    val leftTypes = RelOptUtil.getFieldTypeList(left.getRowType)
    val rightTypes = RelOptUtil.getFieldTypeList(right.getRowType)
    val conditions = keys.map { key =>
      val leftRex = rexBuilder.makeInputRef(leftTypes.get(key), key)
      val rightRex = rexBuilder.makeInputRef(rightTypes.get(key), leftTypes.size + key)
      val equalCond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRex, rightRex)
      relBuilder.or(
        equalCond,
        relBuilder.and(relBuilder.isNull(leftRex), relBuilder.isNull(rightRex)))
    }
    conditions
  }

  /**
    * Use table function to replicate the row N times. First field is long type,
    * and the rest are the row fields.
    */
  def replicateRows(
      builder: RelBuilder, outputType: RelDataType, fields: util.List[Integer]): RelNode = {
    // construct LogicalTableFunctionScan
    val logicalType = toLogicalRowType(outputType)
    val fieldNames = outputType.getFieldNames.toSeq.toArray
    val fieldTypes = logicalType.getChildren.map(fromLogicalTypeToTypeInfo).toArray
    val tf = new ReplicateRows(fieldTypes)
    val resultType = fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames))
    val function = new TypedFlinkTableFunction(tf, fieldNames, resultType)
    val typeFactory = builder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = new TableSqlFunction(
      tf.functionIdentifier,
      tf.toString,
      tf,
      resultType,
      typeFactory,
      function)

    val cluster = builder.peek().getCluster
    val scan = LogicalTableFunctionScan.create(
      cluster,
      new util.ArrayList[RelNode](),
      builder.call(
        sqlFunction,
        builder.fields(Util.range(fields.size() + 1))),
      function.getElementType(null),
      UserDefinedFunctionUtils.buildRelDataType(
        builder.getTypeFactory,
        logicalType,
        fieldNames,
        fieldNames.indices.toArray),
      null)
    builder.push(scan)

    // correlated join
    val corSet = Set(cluster.createCorrel())
    val output = builder
        .join(JoinRelType.INNER, builder.literal(true), corSet)
        .project(builder.fields(Util.range(fields.size() + 1, fields.size() * 2 + 1)))
        .build()
    output
  }
}
