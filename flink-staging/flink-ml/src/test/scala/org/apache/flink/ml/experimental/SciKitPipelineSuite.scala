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

package org.apache.flink.ml.experimental

import org.scalatest.FlatSpec

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.{SparseVector, DenseVector, Vector}
import org.apache.flink.test.util.FlinkTestBase

class SciKitPipelineSuite extends FlatSpec with FlinkTestBase {
  behavior of "Pipeline"

  it should "work" in {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val scaler = new Scaler
    val offset = new Offset

    val input: DataSet[Vector] = env.fromCollection(List(DenseVector(2,1,3), SparseVector.fromCOO(3, (1,1), (2,2))))
    val training = env.fromCollection(List(LabeledVector(1.0, DenseVector(2,3,1)), LabeledVector(2.0, SparseVector.fromCOO(3, (1,1), (2,2)))))
    val intData = env.fromCollection(List(1,2,3,4))

    val result = scaler.transform(input)

    result.print()

    val result2 = offset.transform(input)
    result2.print()

    val chain = scaler.chainTransformer(offset)

    val result3 = chain.transform(input)(ChainedTransformer.chainedTransformOperation(Scaler.vTransform, Offset.offsetTransform))

    result3.print()

    val chain2 = chain.chainTransformer(scaler)
    val result4 = chain2.transform(input)

    result4.print()

    val kmeans = new KMeans()

    val chainedPredictor = chain.chainPredictor(kmeans)

    val prediction = chainedPredictor.predict(result)

    prediction.print()

    env.execute()
  }

}
