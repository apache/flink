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

import scala.util.Sorting

/** Sparse vector implementation storing the data in two arrays. One index contains the sorted
  * indices of the non-zero vector entries and the other the corresponding vector entries
  */
class SparseVector(
    val size: Int,
    val indices: Array[Int],
    val data: Array[Double])
  extends Vector {
  /** Updates the element at the given index with the provided value
    *
    * @param index
    * @param value
    */
  override def update(index: Int, value: Double): Unit = {
    val resolvedIndex = locate(index)

    if (resolvedIndex < 0) {
      throw new IllegalArgumentException("Cannot update zero value of sparse vector at index " +
        index)
    } else {
      data(resolvedIndex) = value
    }
  }

  /** Copies the vector instance
    *
    * @return Copy of the vector instance
    */
  override def copy: Vector = {
    new SparseVector(size, indices.clone, data.clone)
  }

  /** Element wise access function
    *
    * * @param index index of the accessed element
    * @return element with index
    */
  override def apply(index: Int): Double = {
    val resolvedIndex = locate(index)

    if(resolvedIndex < 0) {
      0
    } else {
      data(resolvedIndex)
    }
  }

  def toDenseVector: DenseVector = {
    val denseVector = DenseVector.zeros(size)

    for(index <- 0 until size) {
      denseVector(index) = this(index)
    }

    denseVector
  }

  private def locate(index: Int): Int = {
    require(0 <= index && index < size, s"Index $index is out of bounds [0, $size).")

    java.util.Arrays.binarySearch(indices, 0, indices.length, index)
  }
}

object SparseVector {

  /** Constructs a sparse vector from a coordinate list (COO) representation where each entry
    * is stored as a tuple of (index, value).
    *
    * @param size
    * @param entries
    * @return
    */
  def fromCOO(size: Int, entries: (Int, Double)*): SparseVector = {
    fromCOO(size, entries)
  }

  /** Constructs a sparse vector from a coordinate list (COO) representation where each entry
    * is stored as a tuple of (index, value).
    *
    * @param size
    * @param entries
    * @return
    */
  def fromCOO(size: Int, entries: Iterable[(Int, Double)]): SparseVector = {
    val entryArray = entries.toArray

    val COOOrdering = new Ordering[(Int, Double)] {
      override def compare(x: (Int, Double), y: (Int, Double)): Int = {
        x._1 - y._1
      }
    }

    Sorting.quickSort(entryArray)(COOOrdering)

    // calculate size of the array
    val arraySize = entryArray.foldLeft((-1, 0)){ case ((lastIndex, numRows), (index, _)) =>
      if(lastIndex == index) {
        (lastIndex, numRows)
      } else {
        (index, numRows + 1)
      }
    }._2

    val indices = new Array[Int](arraySize)
    val data = new Array[Double](arraySize)

    val (index, value) = entryArray(0)

    indices(0) = index
    data(0) = value

    var i = 1
    var lastIndex = indices(0)
    var lastDataIndex = 0

    while(i < entryArray.length) {
      val (curIndex, curValue) = entryArray(i)

      if(curIndex == lastIndex) {
        data(lastDataIndex) += curValue
      } else {
        lastDataIndex += 1
        data(lastDataIndex) = curValue
        indices(lastDataIndex) = curIndex
        lastIndex = curIndex
      }

      i += 1
    }

    new SparseVector(size, indices, data)
  }
}
