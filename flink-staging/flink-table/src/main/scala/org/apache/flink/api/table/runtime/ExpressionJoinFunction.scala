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

import org.apache.flink.api.table.tree.{NopExpression, Expression}
import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.codegen.{GenerateBinaryResultAssembler,
GenerateBinaryPredicate}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class ExpressionJoinFunction[L, R, O](
    predicate: Expression,
    leftType: CompositeType[L],
    rightType: CompositeType[R],
    resultType: CompositeType[O],
    outputFields: Seq[Expression]) extends RichFlatJoinFunction[L, R, O] {

  var compiledPredicate: (L, R) => Boolean = null
  var resultAssembler: (L, R, O) => O = null
  var result: O = null.asInstanceOf[O]

  override def open(config: Configuration): Unit = {
    result = resultType.createSerializer(getRuntimeContext.getExecutionConfig).createInstance()
    if (compiledPredicate == null) {
      compiledPredicate = predicate match {
        case n: NopExpression => null
        case _ =>
          val codegen = new GenerateBinaryPredicate[L, R](
            leftType,
            rightType,
            predicate,
            getRuntimeContext.getUserCodeClassLoader)
          codegen.generate()
      }
    }

    if (resultAssembler == null) {
      val resultCodegen = new GenerateBinaryResultAssembler[L, R, O](
        leftType,
        rightType,
        resultType,
        outputFields,
        getRuntimeContext.getUserCodeClassLoader)

      resultAssembler = resultCodegen.generate()
    }
  }

  def join(left: L, right: R, out: Collector[O]) = {
    if (compiledPredicate == null) {
      result = resultAssembler(left, right, result)
      out.collect(result)
    } else {
      if (compiledPredicate(left, right)) {
        result = resultAssembler(left, right, result)
        out.collect(result)      }
    }
  }
}
