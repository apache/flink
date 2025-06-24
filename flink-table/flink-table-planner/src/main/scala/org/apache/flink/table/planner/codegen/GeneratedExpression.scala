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
package org.apache.flink.table.planner.codegen

import org.apache.flink.table.planner.codegen.CodeGenUtils.boxedTypeTermForType
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.types.logical.LogicalType

/**
 * Describes a generated expression.
 *
 * @param resultTerm
 *   term to access the result of the expression
 * @param nullTerm
 *   boolean term that indicates if expression is null
 * @param code
 *   code necessary to produce resultTerm and nullTerm
 * @param resultType
 *   type of the resultTerm
 * @param literalValue
 *   None if the expression is not literal. Otherwise it represent the original object of the
 *   literal.
 */
case class GeneratedExpression(
    resultTerm: String,
    nullTerm: String,
    var code: String,
    resultType: LogicalType,
    literalValue: Option[Any] = None) {

  /**
   * Indicates a constant expression do not reference input and can thus be used in the member area
   * (e.g. as constructor parameter of a reusable instance)
   *
   * @return
   *   true if the expression is literal
   */
  def literal: Boolean = literalValue.isDefined

  /**
   * Copy result term to target term if the reference is changed. Note: We must ensure that the
   * target can only be copied out, so that its object is definitely a brand new reference, not the
   * object being re-used.
   * @param target
   *   the target term that cannot be assigned a reusable reference.
   * @return
   *   code.
   */
  def copyResultTermToTargetIfChanged(ctx: CodeGeneratorContext, target: String): String = {
    if (TypeCheckUtils.isMutable(resultType)) {
      val typeTerm = boxedTypeTermForType(resultType)
      val serTerm = ctx.addReusableTypeSerializer(resultType)
      s"""
         |if ($target != $resultTerm) {
         |  $target = (($typeTerm) $serTerm.copy($resultTerm));
         |}
       """.stripMargin
    } else {
      s"$target = $resultTerm;"
    }
  }

  /**
   * Deep copy the generated expression.
   *
   * NOTE: Please use this method when the result will be buffered. This method makes sure a new
   * object/data is created when the type is mutable.
   */
  def deepCopy(ctx: CodeGeneratorContext): GeneratedExpression = {
    // only copy when type is mutable
    if (TypeCheckUtils.isMutable(resultType)) {
      // if the type need copy, it must be a boxed type
      val typeTerm = boxedTypeTermForType(resultType)
      val serTerm = ctx.addReusableTypeSerializer(resultType)
      val newResultTerm = ctx.addReusableLocalVariable(typeTerm, "field")
      val newCode =
        s"""
           |$code
           |$newResultTerm = $resultTerm;
           |if (!$nullTerm) {
           |  $newResultTerm = ($typeTerm) ($serTerm.copy($newResultTerm));
           |}
        """.stripMargin
      GeneratedExpression(newResultTerm, nullTerm, newCode, resultType, literalValue)
    } else {
      this
    }
  }
}

object GeneratedExpression {
  val ALWAYS_NULL = "true"
  val NEVER_NULL = "false"
  val NO_CODE = ""
}
