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

package org.apache.flink.graph.scala.test

import org.apache.flink.api.scala._
import org.apache.flink.graph.{Edge, Vertex}

object TestGraphUtils {

    def getLongLongVertexData(env: ExecutionEnvironment): DataSet[Vertex[Long, Long]] = {
        env.fromCollection(getLongLongVertices)
    }

    def getLongLongEdgeData(env: ExecutionEnvironment): DataSet[Edge[Long, Long]] = {
        env.fromCollection(getLongLongEdges)
    }

    def getLongLongVertices: List[Vertex[Long, Long]] = {
        List(
            new Vertex[Long, Long](1L, 1L),
            new Vertex[Long, Long](2L, 2L),
            new Vertex[Long, Long](3L, 3L),
            new Vertex[Long, Long](4L, 4L),
            new Vertex[Long, Long](5L, 5L)
        )
    }

    def getLongLongEdges: List[Edge[Long, Long]] = {
        List(
            new Edge[Long, Long](1L, 2L, 12L),
            new Edge[Long, Long](1L, 3L, 13L),
            new Edge[Long, Long](2L, 3L, 23L),
            new Edge[Long, Long](3L, 4L, 34L),
            new Edge[Long, Long](3L, 5L, 35L),
            new Edge[Long, Long](4L, 5L, 45L),
            new Edge[Long, Long](5L, 1L, 51L)
        )
    }
}
