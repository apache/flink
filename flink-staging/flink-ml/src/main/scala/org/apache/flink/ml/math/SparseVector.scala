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

import breeze.linalg.{SparseVector => BreezeSparseVector, DenseVector => BreezeDenseVector, Vector => BreezeVector}

import scala.util.Sorting

/** Sparse vector implementation storing the data in two arrays. One index contains the sorted
  * indices of the non-zero vector entries and the other the corresponding vector entries
  */
case class SparseVector(
    val size: Int,
    val indices: Array[Int],
    val data: Array[Double])
  extends Vector
  with Serializable {
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
  override def copy: SparseVector = {
    new SparseVector(size, indices.clone, data.clone)
  }

  /** Returns the dot product of the recipient and the argument
    *
    * @param other a Vector
    * @return a scalar double of dot product
    */
  override def dot(other: Vector): Double = {
    require(size == other.size, "The size of vector must be equal.")
    other match {
      case DenseVector(otherData) =>
        indices.zipWithIndex.map { case (sparseIdx, idx) => data(idx) * otherData(sparseIdx) }.sum
      case SparseVector(_, otherIndices, otherData) =>
        var left = 0
        var right = 0
        var result = 0.0

        while (left < indices.length && right < otherIndices.length) {
          if (indices(left) < otherIndices(right)) {
            left += 1
          } else if (otherIndices(right) < indices(left)) {
            right += 1
          } else {
            result += data(left) * otherData(right)
            left += 1
            right += 1
          }
        }
        result
    }
  }

  /** Magnitude of a vector
    *
    * @return
    */
  override def magnitude: Double = math.sqrt(data.map(x => x * x).sum)

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

  override def equals(obj: Any): Boolean = {
    obj match {
      case sv: SparseVector if size == sv.size =>
        indices.sameElements(indices) && data.sameElements(sv.data)
      case _ => false
    }
  }

  override def hashCode: Int = {
    val hashCodes = List(size.hashCode, java.util.Arrays.hashCode(indices),
      java.util.Arrays.hashCode(data))

    hashCodes.foldLeft(3){ (left, right) => left * 41 + right}
  }

  override def toString: String = {
    val entries = indices.zip(data).mkString(", ")
    "SparseVector(" + entries + ")"
  }

  private def locate(index: Int): Int = {
    require(0 <= index && index < size, index + " not in [0, " + size + ")")

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

    entryArray.foreach { case (index, _) =>
      require(0 <= index && index < size, index + " not in [0, " + size + ")")
    }

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

  /** Convenience method to be able to instantiate a SparseVector with a single element. The Scala
    * type inference mechanism cannot infer that the second tuple value has to be of type Double
    * if only a single tuple is provided.
    *
    * @param size
    * @param entry
    * @return
    */
  def fromCOO(size: Int, entry: (Int, Int)): SparseVector = {
    fromCOO(size, (entry._1, entry._2.toDouble))
  }

  /** BreezeVectorConverter implementation for [[org.apache.flink.ml.math.SparseVector]]
    *
    * This allows to convert Breeze vectors into [[SparseVector]]
    */
  implicit val sparseVectorConverter = new BreezeVectorConverter[SparseVector] {
    override def convert(vector: BreezeVector[Double]): SparseVector = {
      vector match {
        case dense: BreezeDenseVector[Double] =>
          SparseVector.fromCOO(
            dense.length,
            dense.iterator.toIterable)
        case sparse: BreezeSparseVector[Double] =>
          new SparseVector(
            sparse.used,
            sparse.index.take(sparse.used),
            sparse.data.take(sparse.used))
      }
    }
  }
}
