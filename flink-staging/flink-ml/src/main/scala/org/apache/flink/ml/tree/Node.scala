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

/**
 * Class which defines a node in the Tree.
 * There are two states associated with a node:
 * 1. Trained: The node has been fully grown.
 * a. Leaf: [[predict]] = Some(label) and [[split]] = None
 * b. Internal: [[predict]] = None and [[split]] = Some(splitValue)
 * 2. Untrained: The node is yet to be trained. [[predict]] = None and [[split]] = None
 *
 * IDs of nodes start from 1 at the root level.
 *
 * @param id Node ID
 * @param treeID ID of the tree to which this node belongs
 */

class Node(val id: Int, val treeID: Int) extends Serializable {

  private var __predict: Option[Double] = None
  private var __split: Option[SplitValue] = None

  /**
   * Set a predict value for this node.
   *
   * @param predict Predicted label
   */
  def setPredict(predict: Double): Unit = {
    __predict = Some(predict)
    __split = None
  }

  /**
   * Set a split for this node
   *
   * @param split Split value for this node
   */
  def setSplit(split: SplitValue): Unit = {
    __split = Some(split)
    __predict = None
  }

  /**
   * Gets the predicted label at this node
   *
   * @return Predicted label
   */
  def predict: Option[Double] = __predict

  /**
   * Gets the split value at this node
   *
   * @return Split value
   */
  def split: Option[SplitValue] = __split

  override def toString: String = {
    var ret = s"Node ID: $id, Predict: "
    ret = ret + (predict match {
      case None => "None"
      case Some(value) => value.toString
    })
    ret = ret + ", Split: " + (split match{
      case None => "None"
      case Some(value) => value.toString
    })
    ret
  }
}

object Node {

  /**
   * Return the depth of a node with ID = id
   *
   */
  def getDepth(id: Int): Int = {
    // taking log base 2 of the node id
    // depth starts from one. A matter of convention really
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(id)) + 1
  }

}
