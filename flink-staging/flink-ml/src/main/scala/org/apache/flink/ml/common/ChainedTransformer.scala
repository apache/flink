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

class ChainedTransformer[IN, TEMP, OUT](val head: Transformer[IN, TEMP],
                                        val tail: Transformer[TEMP, OUT])
  extends Transformer[IN, OUT] {

  override def transform(input: DataSet[IN], parameters: ParameterMap): DataSet[OUT] = {
    val tempResult = head.transform(input, parameters)
    tail.transform(tempResult)
  }
}
