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

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.{ParameterMap, LabeledVector}
import org.apache.flink.ml.math._

class KMeans extends Predictor[KMeans] {
}

object KMeans{

  implicit val kMeansEstimator = new FitOperation[KMeans, LabeledVector] {
    override def fit(
        instance: KMeans,
        parameters: ParameterMap,
        input: DataSet[LabeledVector]): Unit = {
      input.print
    }
  }

  implicit def kMeansPredictor[V <: Vector]
    = new PredictOperation[KMeans, V, LabeledVector] {
    override def predict(
        instance: KMeans,
        parameters: ParameterMap,
        input: DataSet[V]): DataSet[LabeledVector] = {
      input.map{
        vector => LabeledVector(1.0, vector)
      }
    }
  }
}
