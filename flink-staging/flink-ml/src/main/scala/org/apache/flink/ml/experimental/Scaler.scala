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

import scala.reflect.ClassTag

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap
import org.apache.flink.ml.math._

class Scaler extends Transformer[Scaler] {
  var meanValue = 0.0
}

object Scaler{
  import Breeze._

  implicit def vTransform[T <: Vector : CanCopy: ClassTag: TypeInformation]
    = new TransformOperation[Scaler, T, T] {
    override def transform(
        instance: Scaler,
        parameters: ParameterMap,
        input: DataSet[T]): DataSet[T] = {
      input.map{
        vector =>
          val breezeVector = copy(vector).asBreeze
          instance.meanValue = instance.meanValue + breeze.stats.mean(breezeVector)

          breezeVector :/= instance.meanValue

          breezeVector.fromBreeze.asInstanceOf[T]
      }
    }
  }
}
