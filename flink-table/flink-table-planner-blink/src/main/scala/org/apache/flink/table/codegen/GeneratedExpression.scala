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

package org.apache.flink.table.codegen

import org.apache.flink.table.`type`.InternalType

/**
  * Describes a generated expression.
  *
  * @param resultTerm term to access the result of the expression
  * @param nullTerm boolean term that indicates if expression is null
  * @param code code necessary to produce resultTerm and nullTerm
  * @param resultType type of the resultTerm
  * @param literal flag to indicate a constant expression do not reference input and can thus
  *                 be used in the member area (e.g. as constructor parameter of a reusable
  *                 instance)
  */
case class GeneratedExpression(
  resultTerm: String,
  nullTerm: String,
  code: String,
  resultType: InternalType,
  literal: Boolean = false)

object GeneratedExpression {
  val ALWAYS_NULL = "true"
  val NEVER_NULL = "false"
  val NO_CODE = ""
}
