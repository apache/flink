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

package org.apache.flink.table.functions

import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.`type`.ReturnTypes
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`type`.OperandTypes
import org.apache.calcite.sql.SqlOperatorBinding
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.LeafExpression

object TimeModeIndicatorFunction {
  
  /**
   * A parameterless scalar function that just indicates processing time mode.
   */
   val PROCTIME = new SqlFunction("PROCTIME", SqlKind.OTHER_FUNCTION,
    ReturnTypes.explicit(SqlTypeName.TIMESTAMP), null, OperandTypes.NILADIC,
    SqlFunctionCategory.SYSTEM) {
    override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

    override def getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity =
      SqlMonotonicity.INCREASING
  }
}
