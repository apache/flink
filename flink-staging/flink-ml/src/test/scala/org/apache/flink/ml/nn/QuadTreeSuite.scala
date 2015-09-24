
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


import org.apache.flink.ml.nn.util.QuadTree
import org.apache.flink.test.util.FlinkTestBase
import org.apache.flink.ml.math.DenseVector

import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable.ListBuffer

/// Test of Quadtree class

/*
Constructor for the Quadtree class
class QuadTree(minVec:ListBuffer[Double], maxVec:ListBuffer[Double]){
*/

class QuadTreeSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "The QuadTree Class"

  it should "construct and search a QuadTree properly" in {

    /////// very basic test of creating a 2D rectangle and a single splitting
    val minVec = ListBuffer(-1.0, -0.5)
    val maxVec = ListBuffer(1.0, 0.5)

    val myTree = new QuadTree(minVec, maxVec)
    println("Created new QuadTree!")

    myTree.insert(DenseVector(-0.25, 0.3))
    println("added 1 element...")
    myTree.insert(DenseVector(-0.20, 0.31))
    println("added 2 element...")
    myTree.insert(DenseVector(-0.21, 0.29))
    println("added 3 element...")

    myTree.insert(DenseVector(0.2, 0.27))
    myTree.insert(DenseVector(0.2, 0.26))

    myTree.insert(DenseVector(-0.21, 0.289))

    myTree.printTree()

    println("testing searchNeighbors....")
    /// need to test search feature................
    //val neighbors = myTree.searchNeighbors(DenseVector(-0.24,0.29), 0.02)
    val neighbors = myTree.searchNeighbors(DenseVector(0.0,0.0), 0.5)
    println("neighbors =    " + neighbors)
    val d = myTree.distance(DenseVector(0.19, 0.27),DenseVector(0.2, 0.27))
    println("d = " + d)

  }
}
