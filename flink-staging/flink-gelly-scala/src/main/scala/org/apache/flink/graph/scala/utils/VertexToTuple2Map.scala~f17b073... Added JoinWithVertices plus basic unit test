package org.apache.flink.graph.scala.utils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.graph.Vertex

class VertexToTuple2Map[K, VV] extends MapFunction[Vertex[K, VV], (K, VV)] {

    private val serialVersionUID: Long = 1L

    override def map(value: Vertex[K, VV]): (K, VV) = {
        (value.getId, value.getValue)
    }
}