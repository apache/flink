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

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.GenerateFilter
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.configuration.Configuration

/**
 * Proxy function that takes an expression predicate. This is compiled
 * upon runtime and calls to [[filter()]] are forwarded to the compiled code.
 */
class ExpressionFilterFunction[T](
    predicate: Expression,
    inputType: CompositeType[T],
    config: TableConfig = TableConfig.DEFAULT) extends RichFilterFunction[T] {

  var compiledFilter: FilterFunction[T] = null

  override def open(c: Configuration): Unit = {
    if (compiledFilter == null) {
      val codegen = new GenerateFilter[T](
        inputType,
        predicate,
        getRuntimeContext.getUserCodeClassLoader,
        config)
      compiledFilter = codegen.generate()
    }
  }

  override def filter(in: T) = compiledFilter.filter(in)
}
