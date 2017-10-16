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

package org.apache.flink.table.calcite

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql._
import org.apache.calcite.sql.validate.{SqlConformanceEnum, SqlValidatorImpl, SqlValidatorScope}
import org.apache.flink.table.api.ValidationException

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

  override def validateJoin(join: SqlJoin, scope: SqlValidatorScope): Unit = {
    // Due to the improperly translation of lateral table left outer join (see CALCITE-2004), we
    // need to temporarily forbid the common predicates until the problem is fixed.
    // The check for join with a lateral table is actually quite tricky.
    if (join.getJoinType == JoinType.LEFT &&
      join.getRight.toString.startsWith("TABLE(")) { // TABLE (`func`(`foo`)) AS...
      join.getCondition match {
        case c: SqlLiteral if c.getValue.equals(true) => // only accept literal true
        case c => throw new ValidationException(s"$c is not a valid predicate for " +
          s"lateral table outer join. Can only accept literal TRUE.")
      }
    }
    super.validateJoin(join, scope)
  }
}
