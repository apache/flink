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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.Indenter._
import org.apache.flink.api.table.expressions.Expression
import org.slf4j.LoggerFactory

/**
 * Code generator for assembling the result of a select operation.
 */
class GenerateSelect[I, O](
    inputTypeInfo: CompositeType[I],
    resultTypeInfo: CompositeType[O],
    outputFields: Seq[Expression],
    cl: ClassLoader,
    config: TableConfig)
  extends GenerateResultAssembler[MapFunction[I, O]](
    Seq(("in0", inputTypeInfo)),
    cl = cl,
    config) {

  val LOG = LoggerFactory.getLogger(this.getClass)

  override protected def generateInternal(): MapFunction[I, O] = {

    val inputTpe = typeTermForTypeInfo(inputTypeInfo)
    val resultTpe = typeTermForTypeInfo(resultTypeInfo)

    val resultCode = createResult(resultTypeInfo, outputFields, o => s"return $o;")

    val generatedName = freshName("GeneratedSelect")

    // Janino does not support generics, that's why we need
    // manual casting here
    val code =
      j"""
        public class $generatedName
            implements org.apache.flink.api.common.functions.MapFunction<$inputTpe, $resultTpe> {

          ${reuseCode(resultTypeInfo)}

          org.apache.flink.api.table.TableConfig config = null;

          public $generatedName(org.apache.flink.api.table.TableConfig config) {
            this.config = config;
            ${reuseInitCode()}
          }

          @Override
          public Object map(Object _in0) {
            $inputTpe in0 = ($inputTpe) _in0;
            $resultCode
          }
        }
      """

    LOG.debug(s"""Generated select:\n$code""")
    compiler.cook(new StringReader(code))
    val clazz = compiler.getClassLoader().loadClass(generatedName)
    val constructor = clazz.getConstructor(classOf[TableConfig])
    constructor.newInstance(config).asInstanceOf[MapFunction[I, O]]
  }
}
