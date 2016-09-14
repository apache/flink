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

package org.apache.flink.ml.optimization

import org.apache.flink.ml.util.FlinkTestBase
import org.scalatest.{FlatSpec, Matchers}

class HuffmanBinaryTreeITSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "HuffmanBinaryTree for use in HSM"

  it should "correctly encode a huffman binary tree given a set of weighted values" in {
    val weightedValues = Range(0, 1000).zip(Range(0, 1000))

    val tree = HuffmanBinaryTree.tree(weightedValues)

    //the most heavily weighted value should have a code that is much shorter than the lightest
    HuffmanBinaryTree.encode(tree, 1000).size should be < HuffmanBinaryTree.encode(tree, 0).size
    //the two lightest values should be sitting on opposite branches of
    // the bottommost node with children
    HuffmanBinaryTree.encode(tree, 0).size should be (HuffmanBinaryTree.encode(tree, 1).size)

    //after encoding all values
    val encodings = weightedValues.map(x => x._1  -> HuffmanBinaryTree.encode(tree, x._1))
    //distinct on paths will result in identities of every inner node
    val paths = encodings.flatMap(x => HuffmanBinaryTree.path(x._2)).distinct

    //there should be one fewer inner nodes than there are values
    paths.size should be (weightedValues.size - 1)
  }
}
