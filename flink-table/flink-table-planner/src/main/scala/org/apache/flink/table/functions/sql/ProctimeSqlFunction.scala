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
package org.apache.flink.table.functions.sql

import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.validate.SqlMonotonicity

/**
  * Function that materializes a processing time attribute.
  * After materialization the result can be used in regular arithmetical calculations.
  */
object ProctimeSqlFunction
  extends SqlFunction(
    "PROCTIME",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
    InferTypes.RETURN_TYPE,
    OperandTypes.family(SqlTypeFamily.TIMESTAMP),
    SqlFunctionCategory.SYSTEM) {

  override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

  override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
    SqlMonotonicity.INCREASING

  override def isDeterministic: Boolean = false
}
