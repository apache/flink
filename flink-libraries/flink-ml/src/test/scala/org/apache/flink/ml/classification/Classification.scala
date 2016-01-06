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

package org.apache.flink.ml.classification

import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object Classification {

  /** Centered data of fisheriris data set
    *
    */
  val trainingData = Seq[LabeledVector](
    LabeledVector(1.0000, DenseVector(-0.2060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.0060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.9060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.3060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.2060, -0.0760)),
    LabeledVector(1.0000, DenseVector(-1.6060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-0.3060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-1.0060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-1.4060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-0.7060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.9060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-0.2060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-1.3060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.5060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.8060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-1.0060, -0.5760)),
    LabeledVector(1.0000, DenseVector(-0.1060, 0.1240)),
    LabeledVector(1.0000, DenseVector(-0.9060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.0060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.2060, -0.4760)),
    LabeledVector(1.0000, DenseVector(-0.6060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.5060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-0.1060, -0.2760)),
    LabeledVector(1.0000, DenseVector(0.0940, 0.0240)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-1.4060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-1.1060, -0.5760)),
    LabeledVector(1.0000, DenseVector(-1.2060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-1.0060, -0.4760)),
    LabeledVector(1.0000, DenseVector(0.1940, -0.0760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.4060, -0.0760)),
    LabeledVector(1.0000, DenseVector(-0.2060, -0.1760)),
    LabeledVector(1.0000, DenseVector(-0.5060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.8060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.9060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.5060, -0.4760)),
    LabeledVector(1.0000, DenseVector(-0.3060, -0.2760)),
    LabeledVector(1.0000, DenseVector(-0.9060, -0.4760)),
    LabeledVector(1.0000, DenseVector(-1.6060, -0.6760)),
    LabeledVector(1.0000, DenseVector(-0.7060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.7060, -0.4760)),
    LabeledVector(1.0000, DenseVector(-0.7060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-0.6060, -0.3760)),
    LabeledVector(1.0000, DenseVector(-1.9060, -0.5760)),
    LabeledVector(1.0000, DenseVector(-0.8060, -0.3760)),
    LabeledVector(-1.0000, DenseVector(1.0940, 0.8240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.2240)),
    LabeledVector(-1.0000, DenseVector(0.9940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(0.6940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(0.8940, 0.5240)),
    LabeledVector(-1.0000, DenseVector(1.6940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(-0.4060, 0.0240)),
    LabeledVector(-1.0000, DenseVector(1.3940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(0.8940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(1.1940, 0.8240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.3240)),
    LabeledVector(-1.0000, DenseVector(0.3940, 0.2240)),
    LabeledVector(-1.0000, DenseVector(0.5940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(0.0940, 0.3240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.7240)),
    LabeledVector(-1.0000, DenseVector(0.3940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.5940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(1.7940, 0.5240)),
    LabeledVector(-1.0000, DenseVector(1.9940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.0940, -0.1760)),
    LabeledVector(-1.0000, DenseVector(0.7940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(-0.0060, 0.3240)),
    LabeledVector(-1.0000, DenseVector(1.7940, 0.3240)),
    LabeledVector(-1.0000, DenseVector(-0.0060, 0.1240)),
    LabeledVector(-1.0000, DenseVector(0.7940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(1.0940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(-0.1060, 0.1240)),
    LabeledVector(-1.0000, DenseVector(-0.0060, 0.1240)),
    LabeledVector(-1.0000, DenseVector(0.6940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(0.8940, -0.0760)),
    LabeledVector(-1.0000, DenseVector(1.1940, 0.2240)),
    LabeledVector(-1.0000, DenseVector(1.4940, 0.3240)),
    LabeledVector(-1.0000, DenseVector(0.6940, 0.5240)),
    LabeledVector(-1.0000, DenseVector(0.1940, -0.1760)),
    LabeledVector(-1.0000, DenseVector(0.6940, -0.2760)),
    LabeledVector(-1.0000, DenseVector(1.1940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.6940, 0.7240)),
    LabeledVector(-1.0000, DenseVector(0.5940, 0.1240)),
    LabeledVector(-1.0000, DenseVector(-0.1060, 0.1240)),
    LabeledVector(-1.0000, DenseVector(0.4940, 0.4240)),
    LabeledVector(-1.0000, DenseVector(0.6940, 0.7240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.2240)),
    LabeledVector(-1.0000, DenseVector(0.9940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.7940, 0.8240)),
    LabeledVector(-1.0000, DenseVector(0.2940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.0940, 0.2240)),
    LabeledVector(-1.0000, DenseVector(0.2940, 0.3240)),
    LabeledVector(-1.0000, DenseVector(0.4940, 0.6240)),
    LabeledVector(-1.0000, DenseVector(0.1940, 0.1240))
  )

  val expectedWeightVector = DenseVector(-1.95, -3.45)
}
