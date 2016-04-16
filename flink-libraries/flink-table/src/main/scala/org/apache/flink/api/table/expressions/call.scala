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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

import org.apache.flink.api.table.validate.ExprValidationResult

/**
  * General expression for unresolved function calls. The function can be a built-in
  * scalar function or a user-defined scalar function.
  */
case class Call(functionName: String, args: Seq[Expression]) extends Expression {

  override def children: Seq[Expression] = args

  override def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    throw new UnresolvedException(s"trying to convert UnresolvedFunction $functionName to RexNode")
  }

  override def toString = s"\\$functionName(${args.mkString(", ")})"

  override def makeCopy(newArgs: Array[AnyRef]): this.type = {
    val copy = Call(
      newArgs(0).asInstanceOf[String],
      newArgs.tail.map(_.asInstanceOf[Expression]))

    copy.asInstanceOf[this.type]
  }

  override def dataType =
    throw new UnresolvedException(s"calling dataType on Unresolved Function $functionName")

  override def validateInput(): ExprValidationResult =
    ExprValidationResult.ValidationFailure(s"Unresolved function call: $functionName")
}
