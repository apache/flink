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

/** Base trait for Vectors
  *
  */
trait Vector {

  /** Number of elements in a vector
    *
    * @return
    */
  def size: Int

  /** Element wise access function
    *
    * * @param index index of the accessed element
    * @return element with index
    */
  def apply(index: Int): Double

  /** Updates the element at the given index with the provided value
    *
    * @param index
    * @param value
    */
  def update(index: Int, value: Double): Unit

  /** Copies the vector instance
    *
    * @return Copy of the vector instance
    */
  def copy: Vector

  override def equals(obj: Any): Boolean = {
    obj match {
      case vector: Vector if size == vector.size =>
        0 until size forall { idx =>
          this(idx) == vector(idx)
        }

      case _ => false
    }
  }
}
