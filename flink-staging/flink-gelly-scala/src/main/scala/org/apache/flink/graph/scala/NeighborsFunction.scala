package org.apache.flink.graph.scala


import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.util.Collector


abstract class NeighborsFunction[K, VV, EV, T] extends org.apache.flink.graph.NeighborsFunction[K, VV, EV, T] {

    def iterateNeighbors(neighbors: Iterable[(K, Edge[K, EV], Vertex[K, VV])], out: Collector[T])

    override def iterateNeighbors(neighbors: java.lang.Iterable[Tuple3[K, Edge[K, EV], Vertex[K, VV]]], out: Collector[T]) = {
        val scalaIterable = scala.collection.JavaConversions.iterableAsScalaIterable(neighbors).map(jtuple => (jtuple.f0, jtuple.f1, jtuple.f2))
        iterateNeighbors(scalaIterable, out)
    }
}
