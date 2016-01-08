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
package org.apache.flink.api.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.ExpressionException

/**
 * General expression for custom function calls (row or aggregate calls).
 */
case class Call(functionName: String, args: Expression*) extends Expression {
  def typeInfo = throw new ExpressionException(s"Unresolved call: $this")

  override def children: Seq[Expression] = args

  override def toString = s"\\$functionName(${args.mkString(", ")})"
}

case class ResolvedRowFunctionCall(
    functionName: String,
    returnType: TypeInformation[_],
    args: Seq[Expression])
  extends Expression {

  def typeInfo = returnType

  override def children: Seq[Expression] = args

  override def toString = s"$functionName(${args.mkString(", ")})"
}
