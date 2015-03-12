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

/** This class represents a chain of multiple [[Transformer]].
  *
  * Calling the method `transform` on this object will first apply the preceding [[Transformer]] to
  * the input data. The resulting output data is then fed to the succeeding [[Transformer]].
  *
  * @param head Preceding [[Transformer]]
  * @param tail Succeeding [[Transformer]]
  * @tparam IN Type of incoming elements
  * @tparam TEMP Type of output elements of the preceding [[Transformer]] and input type of
  *              succeeding [[Transformer]]
  * @tparam OUT Type of outgoing elements
  */
class ChainedTransformer[IN, TEMP, OUT](val head: Transformer[IN, TEMP],
                                        val tail: Transformer[TEMP, OUT])
  extends Transformer[IN, OUT] {

  override def transform(input: DataSet[IN], transformParameters: ParameterMap): DataSet[OUT] = {
    val tempResult = head.transform(input, transformParameters)
    tail.transform(tempResult)
  }
}
