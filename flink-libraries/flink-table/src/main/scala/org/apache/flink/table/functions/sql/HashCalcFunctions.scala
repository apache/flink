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

import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}


object HashCalcFunctions {
  val MD5_CALC = new SqlFunction(
    "MD5",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE_VARYING,
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING
  )

  val SHA1_CALC = new SqlFunction(
    "SHA1",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE_VARYING,
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING
  )

  val SHA256_CALC = new SqlFunction(
    "SHA256",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE_VARYING,
    null,
    OperandTypes.ONE_OR_MORE,
    SqlFunctionCategory.STRING
  )
}
