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
package org.apache.flink.ml.preprocessing.validation

import org.apache.flink.ml.math.BreezeVectorConverter
import org.apache.flink.ml.math.Vector

/** Helper to validate data.
  *
  */
object DataValidation {
  private def isFiniteVectorElement(value: Double): Boolean = {
   !value.isNaN && !value.isInfinity
  }

  /** Checks if a vector contains finite values.
    *
    * @param vector the vector to check
    * @tparam T type of vector
    * @return true if it is valide false otherwise
    */
  def isFiniteVector[T <: Vector: BreezeVectorConverter](vector: T): Boolean = {
    !vector.map{x => isFiniteVectorElement(x._2)}.toList.contains(false)
  }

  /** Checks if a vector contains zero values
    *
    * @param vector the vector to check
    * @tparam T type of vector
    * @return true if it is a zero vector false otherwise
    */
  def isZeroVector[T <: Vector: BreezeVectorConverter](vector: T)(): Boolean = {
    !vector.toList.forall(value => Math.abs(value._2) > 0.0)
  }
}
