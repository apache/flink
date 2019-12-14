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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.types.logical.DecimalType

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.validate.{SqlConformanceEnum, SqlValidatorImpl, SqlValidatorScope}
import org.apache.calcite.util.Static

/**
 * This is a copy of Calcite's CalciteSqlValidator to use with [[FlinkPlannerImpl]].
 */
class FlinkCalciteSqlValidator(
    opTab: SqlOperatorTable,
    catalogReader: CalciteCatalogReader,
    factory: JavaTypeFactory)
  extends SqlValidatorImpl(
    opTab,
    catalogReader,
    factory,
    SqlConformanceEnum.DEFAULT) {

  override def getLogicalSourceRowType(
      sourceRowType: RelDataType,
      insert: SqlInsert): RelDataType = {
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(sourceRowType)
  }

  override def getLogicalTargetRowType(
      targetRowType: RelDataType,
      insert: SqlInsert): RelDataType = {
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(targetRowType)
  }

  override def validateLiteral(literal: SqlLiteral): Unit = {
    literal.getTypeName match {
      case SqlTypeName.DECIMAL =>
        val bd = literal.getValue.asInstanceOf[java.math.BigDecimal]
        if (bd.precision > DecimalType.MAX_PRECISION) {
          throw newValidationError(
            literal,
            Static.RESOURCE.numberLiteralOutOfRange(bd.toString))
        }
      case _ => super.validateLiteral(literal)
    }
  }

  override def validateJoin(join: SqlJoin, scope: SqlValidatorScope): Unit = {
    // Due to the improper translation of lateral table left outer join in Calcite, we need to
    // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
    if (join.getJoinType == JoinType.LEFT &&
      isCollectionTable(join.getRight)) {
      join.getCondition match {
        case c: SqlLiteral if c.booleanValue() && c.getValue.asInstanceOf[Boolean] =>
        // We accept only literal true
        case c if null != c =>
          throw new ValidationException(
            s"Left outer joins with a table function do not accept a predicate such as $c. " +
              s"Only literal TRUE is accepted.")
      }
    }
    super.validateJoin(join, scope)
  }

  private def isCollectionTable(node: SqlNode): Boolean = {
    // TABLE (`func`(`foo`)) AS bar
    node match {
      case n: SqlCall if n.getKind == SqlKind.AS =>
        n.getOperandList.get(0).getKind == SqlKind.COLLECTION_TABLE
      case _ => false
    }
  }
}
