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

package org.apache.flink.ml.math

import breeze.linalg.{Vector => BreezeVector}

/** Type class which allows the conversion from Breeze vectors to Flink vectors
  *
  * @tparam T Resulting type of the conversion
  */
trait BreezeVectorConverter[T <: Vector] extends Serializable {
  /** Converts a Breeze vector into a Flink vector of type T
    *
    * @param vector Breeze vector
    * @return Flink vector of type T
    */
  def convert(vector: BreezeVector[Double]): T
}
