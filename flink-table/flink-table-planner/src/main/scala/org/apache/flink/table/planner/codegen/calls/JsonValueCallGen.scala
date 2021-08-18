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
import org.apache.calcite.sql.{SqlJsonEmptyOrError, SqlJsonValueEmptyOrErrorBehavior}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{BINARY_STRING, qualifyEnum, qualifyMethod}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGenUtils, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}

/**
 * [[CallGenerator]] for [[BuiltInMethods.JSON_VALUE]].
 *
 * We cannot use [[MethodCallGen]] for a few different reasons. First, the return type of the
 * built-in Calcite function is [[Object]] and needs to be cast based on the inferred return type
 * instead as users can change this using the RETURNING keyword. Furthermore, we need to provide
 * the proper default values in case not all arguments were given.
 */
class JsonValueCallGen extends CallGenerator {
  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable = true) {
      argTerms => {
        val emptyBehavior = getBehavior(operands, SqlJsonEmptyOrError.EMPTY)
        val errorBehavior = getBehavior(operands, SqlJsonEmptyOrError.ERROR)
        val terms = Seq(
          s"${argTerms.head}.toString()",
          s"${argTerms(1)}.toString()",
          qualifyEnum(emptyBehavior._1),
          emptyBehavior._2,
          qualifyEnum(errorBehavior._1),
          errorBehavior._2
        )

        val rawResultTerm = CodeGenUtils.newName("rawResult")
        val call = s"""
           |Object $rawResultTerm =
           |    ${qualifyMethod(BuiltInMethods.JSON_VALUE)}(${terms.mkString(", ")});
           """.stripMargin

        val convertedResult = returnType.getTypeRoot match {
          case LogicalTypeRoot.VARCHAR =>
            s"$BINARY_STRING.fromString(java.lang.String.valueOf($rawResultTerm))"
          case LogicalTypeRoot.BOOLEAN => s"(java.lang.Boolean) $rawResultTerm"
          case LogicalTypeRoot.INTEGER => s"(java.lang.Integer) $rawResultTerm"
          case LogicalTypeRoot.DOUBLE => s"(java.lang.Double) $rawResultTerm"
          case _ => throw new CodeGenException(s"Unsupported type '$returnType' "
            + "for RETURNING in JSON_VALUE().")
        }

        val result = s"($rawResultTerm == null) ? null : ($convertedResult)"
        (call, result)
      }
    }
  }

  /**
   * Returns a tuple of behavior and default value for the given mode (EMPTY, ERROR).
   *
   * The operands for this function call are a sequence of the keywords, e.g. "NULL" + "EMPTY", or
   * "DEFAULT" + expression + "ERROR". However, the native Calcite call simply expects four
   * arguments in order (behavior + default value, once for EMPTY and ERROR each).
   */
  private def getBehavior(
      operands: Seq[GeneratedExpression],
      mode: SqlJsonEmptyOrError): (SqlJsonValueEmptyOrErrorBehavior, String) = {
    operands.indexWhere(expr => expr.literalValue.contains(mode)) match {
      case -1 => (SqlJsonValueEmptyOrErrorBehavior.NULL, null)
      case modeIndex => operands(modeIndex - 1).literalValue.get match {
        // Case for [NULL | ERROR] ON [EMPTY | ERROR]
        case behavior: SqlJsonValueEmptyOrErrorBehavior => (behavior, null)
        case _ => operands(modeIndex - 2).literalValue.get match {
          // Case for DEFAULT <expr> ON [EMPTY | ERROR]
          case behavior: SqlJsonValueEmptyOrErrorBehavior =>
            (behavior, operands(modeIndex - 1).resultTerm)
          case _ =>
            throw new CodeGenException("Invalid combination of arguments for JSON_VALUE. "
              + "This is a bug. Please consider filing an issue.")
        }
      }
    }
  }
}
