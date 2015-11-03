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
package org.apache.flink.api.table.runtime

import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.codegen.GenerateSelect
import org.apache.flink.configuration.Configuration

/**
 * Proxy function that takes expressions. These are compiled
 * upon runtime and calls to [[map()]] are forwarded to the compiled code.
 */
class ExpressionSelectFunction[I, O](
     inputType: CompositeType[I],
     resultType: CompositeType[O],
     outputFields: Seq[Expression],
     config: TableConfig = TableConfig.DEFAULT) extends RichMapFunction[I, O] {

  var compiledSelect: MapFunction[I, O] = null

  override def open(c: Configuration): Unit = {

    if (compiledSelect == null) {
      val resultCodegen = new GenerateSelect[I, O](
        inputType,
        resultType,
        outputFields,
        getRuntimeContext.getUserCodeClassLoader,
        config)

      compiledSelect = resultCodegen.generate()
    }
  }

  def map(in: I): O = {
    compiledSelect.map(in)
  }
}
