package org.apache.flink.graph.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.{graph => jg}

import scala.reflect.ClassTag


object Graph {
    def fromDataSet[K: TypeInformation, VV: TypeInformation, EV: TypeInformation](vertices: DataSet[Vertex[K, VV]], edges: DataSet[Edge[K, EV]], env: ExecutionEnvironment): Graph[K, VV, EV] = {
        new Graph[K, VV, EV](jg.Graph.fromDataSet[K, VV, EV](vertices.javaSet, edges.javaSet, env.javaEnv))
    }
}

final class Graph[K: TypeInformation, VV: TypeInformation, EV: TypeInformation](jgraph: jg.Graph[K, VV, EV]) {

    private[flink] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
        if (jgraph.getContext.getConfig.isClosureCleanerEnabled) {
            ClosureCleaner.clean(f, checkSerializable)
        }
        ClosureCleaner.ensureSerializable(f)
        f
    }

    def getVertices = wrap(jgraph.getVertices)

    def getEdges = wrap(jgraph.getEdges)

    def mapVertices[NV: TypeInformation : ClassTag](mapper: MapFunction[Vertex[K, VV], NV]): Graph[K, NV, EV] = {
        new Graph[K, NV, EV](jgraph.mapVertices[NV](
            mapper,
            createTypeInformation[Vertex[K, NV]]
        ))
    }

    def mapVertices[NV: TypeInformation : ClassTag](fun: Vertex[K, VV] => NV): Graph[K, NV, EV] = {
        val mapper: MapFunction[Vertex[K, VV], NV] = new MapFunction[Vertex[K, VV], NV] {
            val cleanFun = clean(fun)

            def map(in: Vertex[K, VV]): NV = cleanFun(in)
        }
        new Graph[K, NV, EV](jgraph.mapVertices[NV](mapper, createTypeInformation[Vertex[K, NV]]))
    }

    def mapEdges[NV: TypeInformation : ClassTag](mapper: MapFunction[Edge[K, EV], NV]): Graph[K, VV, NV] = {
        new Graph[K, VV, NV](jgraph.mapEdges[NV](
            mapper,
            createTypeInformation[Edge[K, NV]]
        ))
    }

    def mapEdges[NV: TypeInformation : ClassTag](fun: Edge[K, EV] => NV): Graph[K, VV, NV] = {
        val mapper: MapFunction[Edge[K, EV], NV] = new MapFunction[Edge[K, EV], NV] {
            val cleanFun = clean(fun)

            def map(in: Edge[K, EV]): NV = cleanFun(in)
        }
        new Graph[K, VV, NV](jgraph.mapEdges[NV](mapper, createTypeInformation[Edge[K, NV]]))
    }
}