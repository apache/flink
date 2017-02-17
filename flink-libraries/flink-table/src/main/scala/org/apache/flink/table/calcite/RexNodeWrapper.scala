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

import org.apache.calcite.rex._
import org.apache.calcite.sql._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.expressions.{Expression, Literal, ResolvedFieldReference}
import org.apache.flink.table.validate.FunctionCatalog
import org.apache.flink.table.calcite.RexNodeWrapper._

abstract class RexNodeWrapper(rex: RexNode) {
  def get: RexNode = rex
  def toExpression(names: Map[RexInputRef, String]): Expression
}

case class RexLiteralWrapper(literal: RexLiteral) extends RexNodeWrapper(literal) {
  override def toExpression(names: Map[RexInputRef, String]): Expression = {
    val typeInfo = FlinkTypeFactory.toTypeInfo(literal.getType)
    Literal(literal.getValue, typeInfo)
  }
}

case class RexInputWrapper(input: RexInputRef) extends RexNodeWrapper(input) {
  override def toExpression(names: Map[RexInputRef, String]): Expression = {
    val typeInfo = FlinkTypeFactory.toTypeInfo(input.getType)
    ResolvedFieldReference(names(input), typeInfo)
  }
}

case class RexCallWrapper(
    call: RexCall,
    operands: Seq[RexNodeWrapper]) extends RexNodeWrapper(call) {

  override def toExpression(names: Map[RexInputRef, String]): Expression = {
    val ops = operands.map(_.toExpression(names))
    call.op match {
      case function: SqlFunction =>
        lookupFunction(replace(function.getName), ops)
      case postfix: SqlPostfixOperator =>
        lookupFunction(replace(postfix.getName), ops)
      case operator@_ =>
        val name = replace(s"${operator.kind}")
        lookupFunction(name, ops)
    }
  }

  def replace(str: String): String = {
    str.replaceAll("\\s|_", "")
  }
}

object RexNodeWrapper {

  private var catalog: Option[FunctionCatalog] = None

  def wrap(rex: RexNode, functionCatalog: FunctionCatalog): RexNodeWrapper = {
    catalog = Option(functionCatalog)
    rex.accept(new WrapperVisitor)
  }

  private[table] def lookupFunction(name: String, operands: Seq[Expression]): Expression = {
    catalog.getOrElse(throw TableException("FunctionCatalog was not defined"))
      .lookupFunction(name, operands)
  }
}

class WrapperVisitor extends RexVisitorImpl[RexNodeWrapper](true) {

  override def visitInputRef(inputRef: RexInputRef): RexNodeWrapper = {
    RexInputWrapper(inputRef)
  }

  override def visitLiteral(literal: RexLiteral): RexNodeWrapper = {
    RexLiteralWrapper(literal)
  }

  override def visitLocalRef(localRef: RexLocalRef): RexNodeWrapper = {
    localRef.accept(this)
  }

  override def visitCall(call: RexCall): RexNodeWrapper = {
    val operands = for {
      x <- 0 until call.operands.size()
    } yield {
      call.operands.get(x).accept(this)
    }
    RexCallWrapper(call, operands)
  }
}
