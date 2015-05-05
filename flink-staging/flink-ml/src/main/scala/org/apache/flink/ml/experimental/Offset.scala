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

class Offset extends Transformer[Offset] {
}

object Offset{
  import Breeze._

  implicit def offsetTransform[I <: Vector : CanCopy: ClassTag: TypeInformation]
    = new TransformOperation[Offset, I, I] {
    override def transform(
        offset: Offset,
        parameters: ParameterMap,
        input: DataSet[I]): DataSet[I] = {
      input.map{
        vector =>
          val brz = copy(vector).asBreeze

          val result = brz + 1.0

          result.fromBreeze.asInstanceOf[I]
      }
    }
  }
}
