
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

/** Test of Quadtree class
  * Constructor for the Quadtree class:
  * class QuadTree(minVec:ListBuffer[Double], maxVec:ListBuffer[Double])
  *
 */

class QuadTreeSuite extends FlatSpec with Matchers with FlinkTestBase {
  behavior of "The QuadTree Class"

  it should "partition into equal size sub-boxes and search for nearby objects properly" in {

    val minVec = ListBuffer(-1.0, -0.5)
    val maxVec = ListBuffer(1.0, 0.5)

    val myTree = new QuadTree(minVec, maxVec)
    myTree.maxPerBox = 3

    /**
     * WANT TO MAKE SURE THAT OBJECTS INSERTED ARE IN BOUNDING BOX??
     * 2 OPTIONS:  RE-BUILD TREE WHEN AN OUTSIDE OBJECT IS ADDED, OR NOT ALLOW OBJECT TO BE ADDED
     *
     */

    myTree.insert(DenseVector(-0.25, 0.3))
    myTree.insert(DenseVector(-0.20, 0.31))
    myTree.insert(DenseVector(-0.21, 0.29))

    /** Tree will partition once the 4th point is added
     */

    myTree.insert(DenseVector(0.2, 0.27))
    myTree.insert(DenseVector(0.2, 0.26))

    myTree.insert(DenseVector(-0.21, 0.289))
    myTree.insert(DenseVector(-0.1, 0.289))

    myTree.insert(DenseVector(0.7, 0.45))

    /**
     * Exact values of (centers,dimensions) of root + children nodes, to test
     * partitionBox and makeChildren methods; exact values are given to avoid
     * essentially copying and pasting the code to automatically generate them
     * from minVec/maxVec
     */

    val knownCentersLengths = Set( (ListBuffer(0.0,0.0), ListBuffer(2.0,1.0)),
      (ListBuffer(-0.5, -0.25), ListBuffer(1.0, 0.5)),
      (ListBuffer(-0.5, 0.25), ListBuffer(1.0, 0.5)),
      (ListBuffer(0.5, -0.25), ListBuffer(1.0, 0.5)),
      (ListBuffer(0.5, 0.25), ListBuffer(1.0, 0.5))
    )

    /**
     * (centers,dimensions) computed from QuadTree.makeChildren
     */

    var computedCentersLength =  Set( ( ListBuffer(0.0,0.0) , ListBuffer(2.0,1.0) ))
    for (child <- myTree.root.children){
     computedCentersLength += child.getCenterLength()

    }

    /**
     * Tests search for nearby neighbors, make sure the right object is contained in neighbor search
     * the neighbor search will contain more points
     */
    val neighborsComputed = myTree.searchNeighbors(DenseVector(0.7001,0.45001), 0.001)
    val isNeighborInSearch = neighborsComputed.contains(DenseVector(0.7, 0.45))

    /**
     * Test ability to get all objects in minimal bounding box + objects in siblings' block method
     *  In this case, drawing a picture of the QuadTree shows that
     *  (-0.2, 0.31), (-0.21, 0.29), (-0.21, 0.289), (-0.1, 0.289)
     *  are objects near (-0.2001, 0.31001)
     */

    val siblingsObjectsComputed = myTree.searchNeighborsSiblingQueue(DenseVector(-0.2001, 0.31001))
    val isSiblingsInSearch = siblingsObjectsComputed.contains(DenseVector(-0.2, 0.31)) &&
      siblingsObjectsComputed.contains(DenseVector(-0.21, 0.29)) &&
      siblingsObjectsComputed.contains(DenseVector(-0.21, 0.289)) &&
      siblingsObjectsComputed.contains( DenseVector(-0.1, 0.289))

    computedCentersLength should be(knownCentersLengths)
    isNeighborInSearch should be(true)
    isSiblingsInSearch should be(true)
  }
}