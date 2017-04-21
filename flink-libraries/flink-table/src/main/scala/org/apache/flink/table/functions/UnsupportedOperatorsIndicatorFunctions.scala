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

import java.nio.charset.Charset
import java.util.List

import org.apache.calcite.rel.`type`._
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.LeafExpression
import org.apache.calcite.sql.`type`.InferTypes
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope

/**
 * An SQL Function DISTINCT(<NUMERIC>) used to mark the DISTINCT operator
 * on aggregation input. This is temporary workaround waiting for 
 * https://issues.apache.org/jira/browse/CALCITE-1740 being solved
 */
object DistinctAggregatorExtractor extends SqlFunction("DIST", SqlKind.OTHER_FUNCTION,
  ReturnTypes.ARG0, InferTypes.RETURN_TYPE,
  OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC) {
  
  override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

}
