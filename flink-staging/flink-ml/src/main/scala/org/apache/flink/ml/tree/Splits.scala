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


package org.apache.flink.ml.tree

import org.apache.flink.ml.math.Vector

trait SplitValue extends Serializable {

  /**
   * Determines whether to send the vector to the left (true) or right (false) tree
   *
   * @param vector Input vector
   */
  def getSplitDirection(vector: Vector): Boolean

}

/**
 * Split for a continuous valued field
 *
 * @param index Index of the attribute this split works on
 * @param splitValue Value which determines the threshold for left and right trees
 */
class ContinuousSplit(val index: Int, val splitValue: Double) extends SplitValue {

  override def getSplitDirection(vector: Vector): Boolean = {
    if (vector.apply(index) < splitValue) true else false
  }

  override def toString: String = {
    s"Continuous Split for index: $index at $splitValue"
  }
}

/**
 * Split for a discrete valued field
 *
 * @param index Index of the attribute this split works on
 * @param splitList List which determines whether the vector goes left or right
 */
class DiscreteSplit(val index: Int, val splitList: Array[Double]) extends SplitValue {

  override def getSplitDirection(vector: Vector): Boolean = {
    if (splitList.contains(vector.apply(index))) true else false
  }

  override def toString: String = {
    s"Discrete split for index: $index at " + splitList.toString
  }
}
