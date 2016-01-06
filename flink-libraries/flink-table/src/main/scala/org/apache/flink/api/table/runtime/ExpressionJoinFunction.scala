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

import org.apache.flink.api.common.functions.{FlatJoinFunction, RichFlatJoinFunction}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.TableConfig
import org.apache.flink.api.table.codegen.GenerateJoin
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * Proxy function that takes an expression predicate and output fields. These are compiled
 * upon runtime and calls to [[join()]] are forwarded to the compiled code.
 */
class ExpressionJoinFunction[L, R, O](
    predicate: Expression,
    leftType: CompositeType[L],
    rightType: CompositeType[R],
    resultType: CompositeType[O],
    outputFields: Seq[Expression],
    config: TableConfig = TableConfig.DEFAULT) extends RichFlatJoinFunction[L, R, O] {

  var compiledJoin: FlatJoinFunction[L, R, O] = null

  override def open(c: Configuration): Unit = {
    val codegen = new GenerateJoin[L, R, O](
      leftType,
      rightType,
      resultType,
      predicate,
      outputFields,
      getRuntimeContext.getUserCodeClassLoader,
      config)
    compiledJoin = codegen.generate()
  }

  def join(left: L, right: R, out: Collector[O]) = {
    compiledJoin.join(left, right, out)
  }
}
