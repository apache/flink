package org.apache.flink.graph.scala


import java.lang

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.util.Collector


abstract class NeighborsFunctionWithVertexValue[K, VV, EV, T] extends org.apache.flink.graph.NeighborsFunctionWithVertexValue[K, VV, EV, T] {

    def iterateNeighbors(vertex: Vertex[K, VV], neighbors: Iterable[(Edge[K, EV], Vertex[K, VV])], out: Collector[T]): Unit

    override def iterateNeighbors(vertex: Vertex[K, VV], neighbors: lang.Iterable[Tuple2[Edge[K, EV], Vertex[K, VV]]], out: Collector[T]): Unit = {
        val scalaIterable = scala.collection.JavaConversions.iterableAsScalaIterable(neighbors).map(jtuple => (jtuple.f0, jtuple.f1))
        iterateNeighbors(vertex, scalaIterable, out)
    }
}
