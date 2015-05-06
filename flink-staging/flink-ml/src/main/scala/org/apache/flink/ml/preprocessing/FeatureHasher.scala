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

package org.apache.flink.ml.preprocessing

import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.{ParameterMap, Parameter, Transformer}
import org.apache.flink.ml.math.{DenseVector, SparseVector, Vector}
import org.apache.flink.ml.preprocessing.FeatureHasher.{NonNegative, NumFeatures}

import scala.util.hashing.MurmurHash3


class FeatureHasher extends Transformer[Vector, Vector] with Serializable {
  // parameters: n_features, non_negative
  def setNumFeatures(numFeatures: Int): FeatureHasher = {
    // parameter check
    parameters.add(NumFeatures, numFeatures)
    this
  }

  def setNonNegative(nonNegative: Boolean): FeatureHasher = {
    parameters.add(NonNegative, nonNegative)
    this
  }

  override def transform(input: DataSet[Vector], parameters: ParameterMap):
  DataSet[Vector] = {
    val resultingParameters = this.parameters ++ parameters
    val nonNegative = resultingParameters(NonNegative)
    val numFeatures = resultingParameters(NumFeatures)

    input.map {
      vector =>
        val v = DenseVector.zeros(numFeatures)
        for (i <- 0 until vector.size) {
          val idx = Math.abs(MurmurHash3.bytesHash(vector(i).getBytes, 0) % numFeatures)
          v(idx) += (if (idx >= 0) 1 else -1)
        }
    }
  }
}

object FeatureHasher {

  case object NumFeatures extends Parameter[Int] {
    override val defaultValue: Option[Int] = Some(Math.pow(2, 18).toInt)
  }

  case object NonNegative extends Parameter[Boolean] {
    override val defaultValue: Option[Boolean] = Some(false)
  }

  def apply(): FeatureHasher = {
    new FeatureHasher()
  }
}