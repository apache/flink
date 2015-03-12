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

/** This class represents a [[org.apache.flink.ml.common.Learner]] which is chained to a
  * [[Transformer]].
  *
  * Calling the method `fit` on this object will pipe the input data through the given
  * [[Transformer]], whose output is fed to the [[Learner]].
  *
  * @param head Preceding [[Transformer]] pipeline
  * @param tail [[Learner]] instance
  * @tparam IN Type of the training data
  * @tparam TEMP Type of the produced data by the transformer pipeline and input type to the
  *              [[Learner]]
  * @tparam OUT Type of the trained model
  */
class ChainedLearner[IN, TEMP, OUT](val head: Transformer[IN, TEMP],
                                    val tail: Learner[TEMP, OUT])
  extends Learner[IN, OUT] {

  override def fit(input: DataSet[IN], fitParameters: ParameterMap): OUT = {
    val tempResult = head.transform(input, fitParameters)

    tail.fit(tempResult, fitParameters)
  }
}
