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
package org.apache.flink.api.expressions.codegen

import org.apache.flink.api.expressions.tree.Expression
import org.apache.flink.api.common.typeutils.CompositeType
import org.slf4j.LoggerFactory

/**
 * Code generator for a unary predicate, i.e. a Filter.
 */
class GenerateUnaryPredicate[T](
    inputType: CompositeType[T],
    predicate: Expression,
    cl: ClassLoader) extends ExpressionCodeGenerator[T => Boolean](
      Seq(("input0", inputType)),
      cl = cl) {

  val LOG = LoggerFactory.getLogger(this.getClass)

  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  override protected def generateInternal(): (T => Boolean) = {
    val pred = generateExpression(predicate)

    val tpe = typeTermForTypeInfo(inputType)

    val code = if (nullCheck) {
      q"""
        (input0: $tpe) => {
          ..${pred.code}
          if (${pred.nullTerm}) {
            false
          } else {
            ${pred.resultTerm}
          }
        }
      """
    } else {
      q"""
        (input0: $tpe) => {
          ..${pred.code}
          ${pred.resultTerm}
        }
      """
    }

    LOG.debug(s"""Generated unary predicate "$predicate":\n$code""")
    toolBox.eval(code).asInstanceOf[(T) => Boolean]
  }
}
