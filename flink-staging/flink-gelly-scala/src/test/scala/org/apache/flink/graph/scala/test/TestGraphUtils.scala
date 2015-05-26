package org.apache.flink.graph.scala.test

import org.apache.flink.api.scala._
import org.apache.flink.graph.{Edge, Vertex}

object TestGraphUtils {

    def getLongLongVertexData(env: ExecutionEnvironment): DataSet[Vertex[Long, Long]] = {
        return env.fromCollection(getLongLongVertices)
    }

    def getLongLongEdgeData(env: ExecutionEnvironment): DataSet[Edge[Long, Long]] = {
        return env.fromCollection(getLongLongEdges)
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
