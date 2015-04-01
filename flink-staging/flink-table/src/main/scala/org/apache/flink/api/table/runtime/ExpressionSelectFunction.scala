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

import org.apache.flink.api.table.tree.Expression
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.codegen.GenerateUnaryResultAssembler
import org.apache.flink.configuration.Configuration

class ExpressionSelectFunction[I, O](
     inputType: CompositeType[I],
     resultType: CompositeType[O],
     outputFields: Seq[Expression]) extends RichMapFunction[I, O] {

  var resultAssembler: (I, O) => O = null
  var result: O = null.asInstanceOf[O]

  override def open(config: Configuration): Unit = {
    result = resultType.createSerializer(getRuntimeContext.getExecutionConfig).createInstance()

    if (resultAssembler == null) {
      val resultCodegen = new GenerateUnaryResultAssembler[I, O](
        inputType,
        resultType,
        outputFields,
        getRuntimeContext.getUserCodeClassLoader)

      resultAssembler = resultCodegen.generate()
    }
  }

  def map(in: I): O = {
    resultAssembler(in, result)
  }
}
