package org.apache.flink.graph.scala


import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.graph.Edge
import org.apache.flink.util.Collector

abstract class EdgesFunction[K, EV, T] extends org.apache.flink.graph.EdgesFunction[K, EV, T] {

    def iterateEdges(edges: Iterable[(K, Edge[K, EV])], out: Collector[T])

    override def iterateEdges(edges: java.lang.Iterable[Tuple2[K, Edge[K, EV]]], out: Collector[T]): Unit = {
        val scalaIterable = scala.collection.JavaConversions.iterableAsScalaIterable(edges).map(jtuple => (jtuple.f0, jtuple.f1))
        iterateEdges(scalaIterable, out)
    }
}
