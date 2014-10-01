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
package org.apache.flink.api.expressions.runtime

import org.apache.flink.api.expressions.codegen.GenerateUnaryPredicate
import org.apache.flink.api.expressions.tree.{NopExpression, Expression}
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.configuration.Configuration

class ExpressionFilterFunction[T](
    predicate: Expression,
    inputType: CompositeType[T]) extends RichFilterFunction[T] {

  var compiledPredicate: (T) => Boolean = null

  override def open(config: Configuration): Unit = {
    if (compiledPredicate == null) {
      compiledPredicate = predicate match {
        case n: NopExpression => null
        case _ =>
          val codegen = new GenerateUnaryPredicate[T](
            inputType,
            predicate,
            getRuntimeContext.getUserCodeClassLoader)
          codegen.generate()
      }
    }
  }

  override def filter(in: T) = compiledPredicate(in)
}
