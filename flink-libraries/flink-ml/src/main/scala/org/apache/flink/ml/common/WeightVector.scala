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

import org.apache.flink.ml.math.Vector

// TODO(tvas): This provides an abstraction for the weights
// but at the same time it leads to the creation of many objects as we have to pack and unpack
// the weights and the intercept often during SGD.

/** This class represents a weight vector with an intercept, as it is required for many supervised
  * learning tasks
  * @param weights The vector of weights
  * @param intercept The intercept (bias) weight
  */
case class WeightVector(weights: Vector, intercept: Double) extends Serializable {}
