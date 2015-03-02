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

package org.apache.flink.ml.math

import org.jblas.DoubleMatrix

/**
 * Convenience functions for the interaction with JBlas. If you want to use JBlas and allow an
 * easy transition from Flink's matrix abstraction to JBlas's and vice versa, simply import
 * all elements contained in the JBlas object.
 */
object JBlas {

  /**
   * Implicit conversion from Flink's [[DenseMatrix]] to JBlas's [[DoubleMatrix]]
   *
   * @param matrix DenseMatrix to be converted
   * @return DoubleMatrix resulting from the given matrix
   */
  implicit def denseMatrix2JBlas(matrix: DenseMatrix): DoubleMatrix = {
    new DoubleMatrix(matrix.numRows, matrix.numCols, matrix.values: _*)
  }

  /**
   * Implicit class to extends [[DoubleMatrix]] such that Flink's [[DenseMatrix]] and
   * [[DenseVector]] can easily be retrieved from.
   * @param matrix
   */
  implicit class RichDoubleMatrix(matrix: DoubleMatrix) {
    def fromJBlas: DenseMatrix = DenseMatrix(matrix.rows, matrix.columns, matrix.data)

    def fromJBlas2Vector: DenseVector = {
      require(matrix.columns == 1, "The JBlas matrix contains more than 1 column.")

      DenseVector(matrix.data)
    }
  }

  /**
   * Implicit conversion from Flink's [[Vector]] to JBlas's [[DoubleMatrix]]
   *
   * @param vector Vector to be converted
   * @return DoubleMatrix resulting from the given vector
   */
  implicit def vector2JBlas(vector: Vector): DoubleMatrix = {
    vector match {
      case x: DenseVector => denseVector2JBlas(x)
    }
  }

  private def denseVector2JBlas(vector: DenseVector): DoubleMatrix = {
    new DoubleMatrix(vector.size, 1, vector.values: _*)
  }
}
