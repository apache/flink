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

package org.apache.flink.ml.common

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml._

/** Base trait for defining convergence criteria
  * User must override the isConverged method
  *
  */
trait Convergence[Data,Model] extends Serializable{

  /** Checks for convergence. Every algorithm will call the checkCOnvergence method to provide
    * all the necessary information to the user defined criterion
    * @param data The data set being used for learning
    * @param prev Solution before the iteration
    * @param curr Solution after the iteration
    *
    */
  final def converged(
      data: DataSet[Data],
      prev: DataSet[Model],
      curr: DataSet[Model])
    : DataSet[_] = {
    isConverged(data, prev, curr)
  }

  /** User must override one of the following two methods and provide an implementation to
    * check if the iteration process has converged. This will be used in conjunction with any
    * other convergence criteria the algorithm designer has in mind.
    * If the iteration must be stopped, user must an empty data set.
    *
    */
  def isConverged(data: DataSet[Data], prev: DataSet[Model], curr: DataSet[Model]): DataSet[_] = {
    isConverged(prev, curr)
  }

  def isConverged(prev: DataSet[Model], curr: DataSet[Model]): DataSet[_] ={
    prev
  }
}
