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
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.`type`.InferTypes
import org.apache.calcite.sql.`type`.OperandTypes
import org.apache.calcite.sql.`type`.ReturnTypes

/**
 * An SQL Function DISTINCT(<ANY>) used to mark the DISTINCT operator
 * on aggregation input. This is temporary workaround waiting for 
 * https://issues.apache.org/jira/browse/CALCITE-1740 being solved
 */
object DistinctAggregatorExtractor extends SqlFunction("DIST", SqlKind.OTHER_FUNCTION,
  ReturnTypes.ARG0, InferTypes.RETURN_TYPE,
  OperandTypes.ANY, SqlFunctionCategory.SYSTEM) {
  
  override def getSyntax: SqlSyntax = SqlSyntax.FUNCTION

}
