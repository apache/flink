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

import org.apache.flink.ml.math.{Histogram, Vector}

import scala.collection.mutable

/** Tree structure. This is kind of maintained in an unconventional way.
  * We provide direct access to all nodes
  * The obvious assumption is that child of i are 2*i and 2*i+1, while parent of i is i/2
  *
  */
class Tree(
            val treeID: Int,
            val nodes: mutable.HashMap[Int, Node],
            val config: TreeConfiguration
            ) extends Serializable {


  override def toString: String = {
    var ret = s"Tree ID=$treeID\nConfiguration:\n$config \nTree Structure:\n"
    for (i <- 1 to Math.pow(2, 20).toInt) {
      if (nodes.get(i).nonEmpty)
        ret = ret + nodes.get(i).get.toString + "\n"
    }
    ret
  }

  /** Determines which node of the tree this vector will go to
    * If predict at any node is -1 and it has a split, we'll go down recursively
    *
    */
  def filter(vector: Vector): (Int, Double) = {
    var node: Node = nodes.get(1).get
    while (node.predict.round.toInt == -1 && node.split.nonEmpty) {
      if (node.split.get.getSplitDirection(vector))
        node = nodes.get(2 * node.id).get
      else
        node = nodes.get(2 * node.id + 1).get
    }
    (node.id, node.predict)
  }
}