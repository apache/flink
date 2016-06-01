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
package org.apache.flink.api.scala

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.operators.base.ReduceOperatorBase
import org.apache.flink.api.java.operators.{ReduceOperator => JavaReduceOperator}

import scala.reflect.ClassTag

@Public
class ReduceOperator[T: ClassTag](javaReduceOperator: JavaReduceOperator[T])
  extends DataSet[T](javaReduceOperator) {

  /**
    * Sets the strategy to use for the combine phase of the reduce.
    *
    * If this method is not called, then the default hint will be used.
    * ([[ org.apache.flink.api.common.operators.base.ReduceOperatorBase.
    * CombineHint.OPTIMIZER_CHOOSES]])
    *
    * @param strategy The hint to use.
    * @return The ReduceOperator object, for function call chaining.
    */
  @PublicEvolving
  def setCombineHint(strategy: ReduceOperatorBase.CombineHint): ReduceOperator[T] = {
    javaReduceOperator.setCombineHint(strategy)
    this
  }

}
