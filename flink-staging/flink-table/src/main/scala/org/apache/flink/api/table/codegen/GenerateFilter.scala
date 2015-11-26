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
package org.apache.flink.api.table.codegen

import java.io.StringReader

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.Indenter._
import org.apache.flink.api.table.expressions.Expression
import org.slf4j.LoggerFactory

/**
 * Code generator for a unary predicate, i.e. a Filter.
 */
class GenerateFilter[T](
    inputType: CompositeType[T],
    predicate: Expression,
    cl: ClassLoader,
    config: TableConfig) extends ExpressionCodeGenerator[FilterFunction[T]](
      Seq(("in0", inputType)),
      cl = cl,
      config) {

  val LOG = LoggerFactory.getLogger(this.getClass)

  override protected def generateInternal(): FilterFunction[T] = {
    val pred = generateExpression(predicate)

    val tpe = typeTermForTypeInfo(inputType)

    val generatedName = freshName("GeneratedFilter")

    // Janino does not support generics, so we need to cast by hand
    val code = if (nullCheck) {
      j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FilterFunction<$tpe> {

          org.apache.flink.api.table.TableConfig config = null;

          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
          }

          public boolean filter(Object _in0) {
            $tpe in0 = ($tpe) _in0;
            ${pred.code}
            if (${pred.nullTerm}) {
              return false;
            } else {
              return ${pred.resultTerm};
            }
          }
        }
      """
    } else {
      j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.FilterFunction<$tpe> {

          org.apache.flink.api.table.TableConfig config = null;

          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
          }

          public boolean filter(Object _in0) {
            $tpe in0 = ($tpe) _in0;
            ${pred.code}
            return ${pred.resultTerm};
          }
        }
      """
    }

    LOG.debug(s"""Generated unary predicate "$predicate":\n$code""")
    compiler.cook(new StringReader(code))
    val clazz = compiler.getClassLoader().loadClass(generatedName)
    val constructor = clazz.getConstructor(classOf[TableConfig])
    constructor.newInstance(config).asInstanceOf[FilterFunction[T]]
  }
}
