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

package org.apache.flink.table.plan.logical

import org.apache.flink.table.expressions.Expression

/**
  * A class implementing this interface can resolve the expressions of its parameters and
  * return a new instance with resolved parameters. This is necessary if expression are nested in
  * a not supported structure. By default, the validation of a logical node can resolve common
  * structures like `Expression`, `Option[Expression]`, `Traversable[Expression]`.
  *
  * See also [[LogicalNode.expressionPostOrderTransform(scala.PartialFunction)]].
  *
  * @tparam T class which expression parameters need to be resolved
  */
trait Resolvable[T <: AnyRef] {

  /**
    * An implementing class can resolve its expressions by applying the given resolver
    * function on its parameters.
    *
    * @param resolver function that can resolve an expression
    * @return class with resolved expression parameters
    */
  def resolveExpressions(resolver: (Expression) => Expression): T
}
