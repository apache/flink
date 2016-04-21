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

package org.apache.flink.graph.scala

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.Utils
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.util.AbstractID

import scala.reflect.ClassTag

package object utils {
  /**
    * This class provides utility methods for computing checksums over a Graph.
    *
    * @param self Graph
    */
  implicit class GraphUtils[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](val self: Graph[K, VV, EV]) {

    /**
     * Convenience method to get the count (number of elements) of a Graph
     * as well as the checksum (sum over element hashes). The vertex and
     * edge DataSets are processed in a single job and the resultant counts
     * and checksums are merged locally.
     *
     * @return the checksum over the vertices and edges
     */
    @throws(classOf[Exception])
    def checksumHashCode(): Utils.ChecksumHashCode = {
      val verticesId = new AbstractID().toString
      self.getVertices.output(new Utils.ChecksumHashCodeHelper[Vertex[K,VV]](verticesId))

      val edgesId = new AbstractID().toString
      self.getEdges.output(new Utils.ChecksumHashCodeHelper[Edge[K,EV]](edgesId))

      val res = self.getWrappedGraph.getContext.execute()

      val checksum = res.getAccumulatorResult[Utils.ChecksumHashCode](verticesId)
      checksum.add(res.getAccumulatorResult[Utils.ChecksumHashCode](edgesId))
      checksum
    }
  }
}
