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

package org.apache.flink.table.expressions

import org.apache.flink.table.validate.FunctionCatalog

import scala.collection.JavaConverters._

/**
  * Bridges between API [[Expression]]s (for both Java and Scala) and final expression stack.
  */
class ExpressionBridge[E <: Expression](
    functionCatalog: FunctionCatalog,
    finalVisitor: ExpressionVisitor[E]) {

  def bridge(expression: Expression): E = {
    // resolve calls
    val resolvedExpressionTree = expression.accept(UnresolvedCallResolver)

    // convert to final expressions
    resolvedExpressionTree.accept(finalVisitor)
  }

  /**
    * Resolves calls with function names to calls with actual function definitions.
    */
  private object UnresolvedCallResolver extends ApiExpressionVisitor[Expression] {

    override def visitTableReference(tableReference: TableReferenceExpression): Expression = {
      tableReference
    }

    override def visitUnresolvedCall(unresolvedCall: UnresolvedCallExpression): Expression = {
      val resolvedDefinition = functionCatalog.lookupFunction(unresolvedCall.getUnresolvedName)
      new CallExpression(
        resolvedDefinition,
        unresolvedCall.getChildren.asScala.map(_.accept(this)).asJava)
    }

    override def visitCall(call: CallExpression): Expression = {
      new CallExpression(
        call.getFunctionDefinition,
        call.getChildren.asScala.map(_.accept(this)).asJava)
    }

    override def visitSymbol(symbolExpression: SymbolExpression): Expression = {
      symbolExpression
    }

    override def visitValueLiteral(valueLiteralExpression: ValueLiteralExpression): Expression = {
      valueLiteralExpression
    }

    override def visitFieldReference(fieldReference: FieldReferenceExpression): Expression = {
      fieldReference
    }

    override def visitTypeLiteral(typeLiteral: TypeLiteralExpression): Expression = {
      typeLiteral
    }

    override def visitNonApiExpression(other: Expression): Expression = {
      // unsupported expressions (e.g. planner expressions) can pass unmodified
      other
    }
  }
}
