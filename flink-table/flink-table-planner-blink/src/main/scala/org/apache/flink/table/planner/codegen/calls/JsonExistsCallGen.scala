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

package org.apache.flink.table.planner.codegen.calls

import java.util.regex.Pattern

import org.apache.calcite.runtime.JsonFunctions.PathMode
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isBinaryString, isCharacterString}
import org.apache.flink.table.types.logical.LogicalType

class JsonExistsCallGen extends CallGenerator {
  private val JSON_PATH_BASE: Pattern = Pattern.compile("""^.*(strict|lax)(.*)$""",
    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE)

  override def generate(
    ctx: CodeGeneratorContext,
    operands: Seq[GeneratedExpression],
    returnType: LogicalType): GeneratedExpression = {
    if (operands(1).literalValue.isEmpty) {
      throw new ValidationException("Illegal jsonpath should be a constant expression!")
    }
    val pathSpec = operands(1).literalValue.get.toString
    val matcher = JSON_PATH_BASE.matcher(pathSpec)
    if (!matcher.matches) {
      throw new ValidationException("Illegal jsonpath spec: " + pathSpec
        + ", format of the spec should be: '<lax|strict> ${expr}'")
    }

    // strict|lax mode
    val mode = matcher.group(1)
    if (mode.toUpperCase.equals(PathMode.LAX.name()) && operands.size == 3) {
      throw new ValidationException("Illegal operation, "
        + "JSON_EXISTS no need to fill in the error behavior in lax mode!")
    }

    val generator = operands match {
      case operands if operands.size == 2 && isBinaryString(operands.head.resultType)
        && isCharacterString(operands(1).resultType) =>
        new MethodCallGen(BuiltInMethods.JSON_EXISTS).generate(ctx, operands, returnType)
      case operands if operands.size == 2 && isCharacterString(operands.head.resultType)
        && isCharacterString(operands(1).resultType) =>
        new MethodCallGen(BuiltInMethods.JSON_EXISTS).generate(ctx, operands, returnType)
      case operands if operands.size == 3 && isBinaryString(operands.head.resultType)
        && isCharacterString(operands(1).resultType) =>
        new MethodCallGen(BuiltInMethods.JSON_EXISTS_ERROR_BEHAVIOR)
          .generate(ctx, operands, returnType)
      case operands if operands.size == 3 && isCharacterString(operands.head.resultType)
        && isCharacterString(operands(1).resultType) =>
        new MethodCallGen(BuiltInMethods.JSON_EXISTS_ERROR_BEHAVIOR)
          .generate(ctx, operands, returnType)
      case _ => throw new ValidationException(s"Illegal Parameter Type error : " +
        s"JSON_EXISTS(${operands.map(_.resultType).mkString(", ")})")
    }

    generator
  }
}
