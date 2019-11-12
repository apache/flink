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

import org.apache.flink.table.planner.codegen.CodeGenUtils.DECIMAL_TERM
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGenUtils, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isNumeric
import org.apache.flink.table.types.logical.{DecimalType, LogicalType}

// DIV(T1, T2) - return integral part of the division; fractional truncated.
//   T1, T2 are exact numeric types.
//   return type has scale=0, and precision big enough.
//     if both T1 and T2 are java primitive types, return T1
//     otherwise, T1 or T2 is Decimal, return Decimal with scale=0
class DivCallGen extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val (arg1, type1) = (operands.head.resultTerm, operands.head.resultType)
    val (arg2, type2) = (operands(1).resultTerm, operands(1).resultType)
    def toDec(arg: String) = s"$DECIMAL_TERM.castFrom($arg, 19, 0)"
    def decDiv(arg1: String, arg2: String) = {
      val dt = returnType.asInstanceOf[DecimalType]
      s"$DECIMAL_TERM.divideToIntegralValue($arg1, $arg2, ${dt.getPrecision}, ${dt.getScale})"
    }

    val code = (type1, type2) match {
      case (_: DecimalType, _: DecimalType) =>
        decDiv(arg1, arg2)

      case (_: DecimalType, right) if isNumeric(right) =>
        decDiv(arg1, toDec(arg2))

      case (left, _: DecimalType) if isNumeric(left) =>
        decDiv(toDec(arg1), arg2)

      case (left, right) if isNumeric(left) && isNumeric(right) =>
        val javaT0 = CodeGenUtils.primitiveTypeTermForType(type1)
        s"($javaT0)( ((long)$arg1) / ((long)$arg2) )"

      case _ =>
        throw new AssertionError(s"Unexpected types ($type1, $type2)")
    }

    generateCallIfArgsNotNull(ctx, returnType, operands) { _ => code }
  }
}
