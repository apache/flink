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

/** Base trait for an algorithm which trains a model based on some training data
  *
  * The idea is that all algorithms which train a model implement this trait. That way
  * they can be chained with [[Transformer]] which act as a preprocessing step for the actual
  * learning. In that sense, [[Learner]] denote the end of a pipeline and cannot be further
  * chained.
  *
  * Every learner has to implement the `fit` method which takes the training data and learns
  * a model from the data.
  *
  * @tparam IN Type of the training data
  * @tparam OUT Type of the trained model
  */
trait Learner[IN, OUT] extends WithParameters {
  def fit(input: DataSet[IN], fitParameters: ParameterMap = ParameterMap.Empty): OUT
}
