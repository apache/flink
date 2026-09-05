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

import org.apache.flink.table.api.{JsonQueryOnEmptyOrError, JsonQueryWrapper}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, CodeGenException, CodeGenUtils, GeneratedExpression}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{qualifyEnum, qualifyMethod, BINARY_STRING, GENERIC_ARRAY}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallWithStmtIfArgsNotNull
import org.apache.flink.table.runtime.functions.SqlJsonUtils
import org.apache.flink.table.runtime.functions.SqlJsonUtils.JsonQueryReturnType
import org.apache.flink.table.types.logical.{ArrayType, DecimalType, LogicalType, LogicalTypeRoot}

/**
 * [[CallGenerator]] for [[BuiltInMethods.JSON_QUERY]].
 *
 * We cannot use [[MethodCallGen]] for a few different reasons. First, the return type of the
 * built-in Calcite function is [[Object]] and needs to be cast based on the inferred return type
 * instead as users can change this using the RETURNING keyword.
 *
 * When multiple JSON function calls share the same input expression, the parsed JSON context is
 * reused via a shared member variable. For example, a query like:
 * {{{
 * SELECT JSON_VALUE(json_data, '$.type'), JSON_QUERY(json_data, '$.address') FROM t
 * }}}
 * generates code similar to:
 * {{{
 * // members (declared once)
 * SqlJsonUtils.JsonValueContext jsonParsed$0;
 * BinaryStringData jsonParsedInput$1;
 *
 * // parses once per input value and reuses the result for the same input
 * private SqlJsonUtils.JsonValueContext parseJson$2(BinaryStringData in) {
 *   if (in != jsonParsedInput$1) {
 *     jsonParsedInput$1 = in;
 *     jsonParsed$0 = SqlJsonUtils.jsonParse(in.toString());
 *   }
 *   return jsonParsed$0;
 * }
 *
 * // whichever call runs first parses, the other ones reuse the result
 * Object rawResult$3 = SqlJsonUtils.jsonValue(parseJson$2(field$0), "$.type", ...);
 * Object rawResult$4 = SqlJsonUtils.jsonQuery(parseJson$2(field$0), "$.address", ...);
 * }}}
 */
class JsonQueryCallGen extends CallGenerator {
  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {

    val parsed = JsonParseReuse.parseSharedInput(ctx, operands)

    generateCallWithStmtIfArgsNotNull(ctx, returnType, operands, resultNullable = true) {
      argTerms =>
        {
          val emptyBehavior = operands(3).literalValue.get.asInstanceOf[JsonQueryOnEmptyOrError]
          val errorBehavior = operands(4).literalValue.get.asInstanceOf[JsonQueryOnEmptyOrError]
          val wrapperBehavior = operands(2).literalValue.get.asInstanceOf[JsonQueryWrapper]

          // Three return paths:
          //   VARCHAR         -> STRING    -> serialize to StringData
          //   ARRAY<VARCHAR>  -> ARRAY     -> GenericArrayData of StringData (existing)
          //   ARRAY<typed T>  -> RAW_ARRAY -> Object[] post-converted via convertJsonArray
          val (isTypedArray, elementType) = returnType.getTypeRoot match {
            case LogicalTypeRoot.ARRAY =>
              val et = returnType.asInstanceOf[ArrayType].getElementType
              (et.getTypeRoot != LogicalTypeRoot.VARCHAR, et)
            case _ => (false, null)
          }

          val jsonQueryReturnType = returnType.getTypeRoot match {
            case LogicalTypeRoot.ARRAY if isTypedArray => JsonQueryReturnType.RAW_ARRAY
            case LogicalTypeRoot.ARRAY => JsonQueryReturnType.ARRAY
            case _ => JsonQueryReturnType.STRING
          }

          val terms = Seq(
            parsed.resultTerm,
            s"${argTerms(1)}.toString()",
            qualifyEnum(jsonQueryReturnType),
            qualifyEnum(wrapperBehavior),
            qualifyEnum(emptyBehavior),
            qualifyEnum(errorBehavior)
          )

          val rawResultTerm = CodeGenUtils.newName(ctx, "rawResult")
          val baseCall = s"""
                            |Object $rawResultTerm =
                            |    ${qualifyMethod(BuiltInMethods.JSON_QUERY_PARSED)}(${terms
                             .mkString(", ")});
           """.stripMargin

          val (additionalCall, convertedResult) = returnType.getTypeRoot match {
            case LogicalTypeRoot.VARCHAR =>
              ("", s"$BINARY_STRING.fromString(java.lang.String.valueOf($rawResultTerm))")
            case LogicalTypeRoot.ARRAY if !isTypedArray =>
              ("", s"($GENERIC_ARRAY) $rawResultTerm")
            case LogicalTypeRoot.ARRAY
                if !SqlJsonUtils.isSupportedJsonReturningType(elementType.getTypeRoot) =>
              throw new CodeGenException(
                s"Unsupported element type '${elementType.getTypeRoot}' "
                  + "for RETURNING ARRAY in JSON_QUERY().")
            case LogicalTypeRoot.ARRAY =>
              val jsonUtils =
                "org.apache.flink.table.runtime.functions.SqlJsonUtils"
              val typeRootEnum =
                s"org.apache.flink.table.types.logical.LogicalTypeRoot.${elementType.getTypeRoot.name()}"
              val (precisionStr, scaleStr) = elementType.getTypeRoot match {
                case LogicalTypeRoot.DECIMAL =>
                  val dt = elementType.asInstanceOf[DecimalType]
                  (dt.getPrecision.toString, dt.getScale.toString)
                case _ => ("0", "0")
              }
              val errorBehaviorEnum = qualifyEnum(errorBehavior)

              val elementNullableStr = elementType.isNullable.toString
              val convertedTerm = CodeGenUtils.newName(ctx, "convertedArr")
              val conversionCode =
                s"""
                   |$GENERIC_ARRAY $convertedTerm = $jsonUtils.convertJsonArray(
                   |    $rawResultTerm, $typeRootEnum,
                   |    $precisionStr, $scaleStr,
                   |    $elementNullableStr,
                   |    $errorBehaviorEnum);
                 """.stripMargin

              (conversionCode, s"$convertedTerm")
            case _ =>
              throw new ValidationException(
                s"Unsupported type '$returnType' "
                  + "for RETURNING in JSON_QUERY().")
          }

          val call = baseCall + additionalCall
          val result = s"($rawResultTerm == null) ? null : ($convertedResult)"
          (call, result)
        }
    }
  }
}
