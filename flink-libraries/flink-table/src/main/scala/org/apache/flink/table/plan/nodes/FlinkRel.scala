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

package org.apache.flink.table.plan.nodes

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlAsOperator
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.runtime.MapRunner

import scala.collection.JavaConversions._

trait FlinkRel {

  private[flink] def getExpressionString(
    expr: RexNode,
    inFields: List[String],
    localExprsTable: Option[List[RexNode]]): String = {

    expr match {
      case i: RexInputRef =>
        inFields.get(i.getIndex)

      case l: RexLiteral =>
        l.toString

      case l: RexLocalRef if localExprsTable.isEmpty =>
        throw new IllegalArgumentException("Encountered RexLocalRef without " +
          "local expression table")

      case l: RexLocalRef =>
        val lExpr = localExprsTable.get(l.getIndex)
        getExpressionString(lExpr, inFields, localExprsTable)

      case c: RexCall =>
        val op = c.getOperator.toString
        val ops = c.getOperands.map(getExpressionString(_, inFields, localExprsTable))
        c.getOperator match {
          case _ : SqlAsOperator => ops.head
          case _ => s"$op(${ops.mkString(", ")})"
        }

      case fa: RexFieldAccess =>
        val referenceExpr = getExpressionString(fa.getReferenceExpr, inFields, localExprsTable)
        val field = fa.getField.getName
        s"$referenceExpr.$field"

      case _ =>
        throw new IllegalArgumentException(s"Unknown expression type '${expr.getClass}': $expr")
    }
  }

  private[flink] def estimateRowSize(rowType: RelDataType): Double = {
    val fieldList = rowType.getFieldList

    fieldList.map(_.getType).foldLeft(0.0) { (s, t) =>
      s + estimateDataTypeSize(t)
    }
  }

  private[flink] def estimateDataTypeSize(t: RelDataType): Double = t.getSqlTypeName match {
    case SqlTypeName.TINYINT => 1
    case SqlTypeName.SMALLINT => 2
    case SqlTypeName.INTEGER => 4
    case SqlTypeName.BIGINT => 8
    case SqlTypeName.BOOLEAN => 1
    case SqlTypeName.FLOAT => 4
    case SqlTypeName.DOUBLE => 8
    case SqlTypeName.VARCHAR => 12
    case SqlTypeName.CHAR => 1
    case SqlTypeName.DECIMAL => 12
    case typeName if SqlTypeName.YEAR_INTERVAL_TYPES.contains(typeName) => 8
    case typeName if SqlTypeName.DAY_INTERVAL_TYPES.contains(typeName) => 4
    case SqlTypeName.TIME | SqlTypeName.TIMESTAMP | SqlTypeName.DATE => 12
    case SqlTypeName.ROW => estimateRowSize(t)
    case SqlTypeName.ARRAY =>
      // 16 is an arbitrary estimate
      estimateDataTypeSize(t.getComponentType) * 16
    case SqlTypeName.ANY => 128 // 128 is an arbitrary estimate
    case _ => throw TableException(s"Unsupported data type encountered: $t")
  }
}
