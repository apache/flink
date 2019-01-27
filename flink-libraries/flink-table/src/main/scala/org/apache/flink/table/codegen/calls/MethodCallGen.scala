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

package org.apache.flink.table.codegen.calls

import java.lang.{Boolean => JBoolean, Byte => JByte, Character => JChar, Short => JShort,
  Integer => JInt, Long => JLong, Float => JFloat, Double => JDouble}
import java.lang.reflect.Method

import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.codegen.CodeGenUtils.qualifyMethod
import org.apache.flink.table.codegen.CodeGeneratorContext.BINARY_STRING
import org.apache.flink.table.codegen.calls.CallGenerator.{generateCallIfArgsNotNull,
  generateCallIfArgsNullable}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

/**
  * Generates a function call by using a [[java.lang.reflect.Method]].
  */
class MethodCallGen(
    method: Method,
    argNotNull: Boolean = true) extends CallGenerator {
  val boxedTypes = Seq(classOf[JByte], classOf[JShort], classOf[JInt],
    classOf[JLong], classOf[JFloat], classOf[JDouble], classOf[JBoolean], classOf[JChar])

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: InternalType,
      nullCheck: Boolean): GeneratedExpression = {

    val call = (origTerms: Seq[String]) => {
      val terms = origTerms.zipWithIndex.map { case (term, index) =>
        if (operands(index).resultType == DataTypes.STRING) {
          s"$term.toString()"
        } else {
          term
        }
      }

      if (FunctionGenerator.isFunctionWithTimeZone(method)) {
        val timeZone = ctx.addReusableTimeZone()
        // insert the zoneID parameters for timestamp functions
        s"""
           |${qualifyMethod(method)}(${terms.mkString(", ")}, $timeZone)
           |""".stripMargin

      } else {
        s"""
           |${qualifyMethod(method)}(${terms.mkString(", ")})
           |""".stripMargin
      }
    }

    val resultCall = if (returnType == DataTypes.STRING ) {
      (args: Seq[String]) => {
        s"$BINARY_STRING.fromString(${call(args)})"
      }
    } else {
      call
    }

    val returnBoxedType = isReturnTypeBoxed(method)
    (argNotNull, returnBoxedType) match {
      case (true, true) => generateCallIfArgsNotNull(ctx, nullCheck, returnType, operands,
        primitiveNullable = true)(resultCall)
      case (true, false) => generateCallIfArgsNotNull(ctx, nullCheck, returnType,
        operands)(resultCall)
      case (false, true) => generateCallIfArgsNullable(ctx, nullCheck, returnType, operands,
        primitiveNullable = true)(resultCall)
      case (false, false) => generateCallIfArgsNullable(ctx, nullCheck, returnType,
        operands)(resultCall)
    }
  }

  def isReturnTypeBoxed(method: Method): Boolean = {
    boxedTypes.contains(method.getReturnType)
  }
}
