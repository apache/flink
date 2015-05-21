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

/** If the node has been trained, it will have:
  * a. predict >=0, in this case, split should be empty. This is fully grown node and we can't go further down
  * b. predict = -1, in this case if split is empty, we need to split, otherwise, this is an internal node
  *
  * ID starts from 1 for the root node.
  * treeID is the tree to which this node belongs
  *
  */

class Node(
            val id: Int,
            val treeID: Int,
            var split: Option[SplitValue],
            var predict: Double = -1
            ) extends Serializable {

  override def toString: String = {
    s"ID=$id, Tree ID=$treeID, predict=$predict, split=$split"
  }

  def getDepth: Int = {
    // taking log base 2 of the node id
    // depth starts from one. A matter of convention really
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(id)) + 1
  }
}