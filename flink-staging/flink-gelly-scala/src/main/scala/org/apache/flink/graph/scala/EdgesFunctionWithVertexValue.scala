package org.apache.flink.graph.scala

import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.util.Collector

abstract class EdgesFunctionWithVertexValue[K, VV, EV, T] extends org.apache.flink.graph.EdgesFunctionWithVertexValue[K, VV, EV, T] {
    @throws(classOf[Exception])
    def iterationFunction(v: Vertex[K, VV], edges: Iterable[Edge[K, EV]], out: Collector[T])

    override def iterateEdges(v: Vertex[K, VV], edges: java.lang.Iterable[Edge[K, EV]], out: Collector[T]) = {
        iterationFunction(v, scala.collection.JavaConversions.iterableAsScalaIterable(edges), out)
    }
}
