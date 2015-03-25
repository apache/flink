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

package org.apache.flink.ml

/**
 * Convenience methods to handle Flink's [[org.apache.flink.ml.math.Matrix]] and [[Vector]]
 * abstraction.
 */
package object math {
  implicit class RichMatrix(matrix: Matrix) extends Iterable[Double] {

    override def iterator: Iterator[Double] = {
      matrix match {
        case dense: DenseMatrix => dense.data.iterator
      }
    }
  }

  implicit class RichVector(vector: Vector) extends Iterable[Double] {
    override def iterator: Iterator[Double] = {
      vector match {
        case dense: DenseVector => dense.data.iterator
      }
    }
  }

  implicit def vector2Array(vector: Vector): Array[Double] = {
    vector match {
      case dense: DenseVector => dense.data
    }
  }
}
