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

import org.apache.flink.table.planner.codegen.CodeGenUtils.{BINARY_STRING, qualifyMethod, SQL_TIMESTAMP}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateCallIfArgsNotNull
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.types.logical.{LogicalType, LogicalTypeRoot}
import java.lang.reflect.Method
import java.util.TimeZone

class MethodCallGen(method: Method) extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, operands, !method.getReturnType.isPrimitive) {
      originalTerms => {
        val terms = originalTerms.zipWithIndex.zip(method.getParameterTypes).map {
          case ((term, i), clazz) =>
            // convert the BinaryString parameter to String if the method parameter accept String
            if (clazz == classOf[String]) {
              s"$term.toString()"
            } else if ((clazz == classOf[Long] || clazz == classOf[java.lang.Long]) &&
                operands(i).resultType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
              // convert the SqlTimestamp parameter to long if the method parameter accept long
              s"$term.getMillisecond()"
            } else {
              term
            }
        }

        // generate method invoke code and adapt when it's a time zone related function
        val call = if (terms.length + 1 == method.getParameterCount &&
          method.getParameterTypes()(terms.length) == classOf[TimeZone]) {
          // insert the zoneID parameters for timestamp functions
          val timeZone = ctx.addReusableTimeZone()
          s"""
             |${qualifyMethod(method)}(${terms.mkString(", ")}, $timeZone)
           """.stripMargin
        } else {
          s"""
             |${qualifyMethod(method)}(${terms.mkString(", ")})
           """.stripMargin
        }

        // convert String to BinaryString if the return type is String
        if (method.getReturnType == classOf[String]) {
          s"$BINARY_STRING.fromString($call)"
        } else if ((method.getReturnType == classOf[Long]
            || method.getReturnType == classOf[java.lang.Long]) &&
            returnType.getTypeRoot == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
          // convert long to SqlTimestamp if the return type is SqlTimestamp and the method
          // returns long
          s"$SQL_TIMESTAMP.fromEpochMillis($call)"
        } else {
          call
        }
      }

    }
  }
}
