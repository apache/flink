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
package org.apache.flink.api.table.expressions.analysis

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.trees.Rule
import org.apache.flink.api.table.{ExpressionException, FunctionSignature, TableConfig}

import scala.collection.mutable

/**
 * Rule that resolves row function calls. It casts arguments if necessary.
 * This rule verifies that the function's signature points to an existing function and
 * creates [[org.apache.flink.api.table.expressions.ResolvedRowFunctionCall]]s that holds the
 * function's return type information in addition to the function name and casted arguments.
 */
class ResolveRowFunctionCalls(config: TableConfig) extends Rule[Expression] {

  def apply(expr: Expression) = {
    val errors = mutable.MutableList[String]()

    val result = expr.transformPost {
      case call@Call(functionName, argExprs@_*) =>
        val argsTypeInfo = argExprs.map(_.typeInfo)

        val functions = config.getRegisteredRowFunctions
        
        // try to find exact signature
        val exactSig = FunctionSignature(functionName, argsTypeInfo)
        if (functions.contains(exactSig)) {
          ResolvedRowFunctionCall(functionName, functions(exactSig).returnType, argExprs)
        }
        // try to find same number of arguments and check if casting is possible
        else {
          val similarFunctions = functions.filterKeys((sig) =>
            sig.name == functionName && sig.args.size == argExprs.size)
          // function name and number of arguments is unique
          if (similarFunctions.size == 1) {
            val similarFunction = similarFunctions.head
            val similarSig = similarFunction._1
            val similarInfo = similarFunction._2

            val castedArgExprs = similarSig.args.zip(argExprs).map(expectedInfoAndExpr => {
              val expectedType = expectedInfoAndExpr._1
              val actualExpr = expectedInfoAndExpr._2
              val actualType = actualExpr.typeInfo

              // no casting necessary
              if (expectedType == actualType) {
                actualExpr
              }
              // casting of basic types
              else if (expectedType != actualType
                  && expectedType.isBasicType && actualType.isBasicType) {
                // do casting
                if (actualType.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
                    expectedType.asInstanceOf[BasicTypeInfo[_]])) {
                  Cast(actualExpr, expectedType)
                }
                // casting not possible
                else {
                  errors += s"Incompatible types for '$functionName'. " +
                    s"Expected: $expectedType, Actual: $actualType"
                  actualExpr
                }
              }
              // casting not possible
              else {
                errors += s"Incompatible types for '$functionName'. " +
                    s"Expected: $expectedType, Actual: $actualType"
                actualExpr
              }
            })
            ResolvedRowFunctionCall(functionName, similarInfo.returnType, castedArgExprs)
          }
          // not unique
          else if (similarFunctions.size > 1) {
            errors += s"Function call '$functionName' is ambiguous."
            call
          }
          // not existing
          else {
            errors += s"Function call '$functionName' is invalid: ${argExprs.mkString(" ")}."
            call
          }
        }
    }

    if (errors.length > 0) {
      throw new ExpressionException(
        s"""Invalid expression "$expr": ${errors.mkString(" ")}""")
    }

    result

  }
}
