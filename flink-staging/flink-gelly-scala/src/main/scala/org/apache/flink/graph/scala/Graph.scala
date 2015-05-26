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
}

object Test {

    def main(args: Array[String]): Unit = {
        runWithPrimitives
        runWithCases
    }

    def runWithPrimitives = {
        // GRAPH CREATION
        val ee = ExecutionEnvironment.createLocalEnvironment(2)
        val vertices = ee.fromElements[Vertex[Long, Long]](new Vertex(1L, 1L), new Vertex(2L, 2L))
        val edges = ee.fromElements[Edge[Long, Long]](new Edge(1L, 2L, 3L))
        val graph = Graph.fromDataSet[Long, Long, Long](vertices, edges, ee)

        val mapper = new CustomMap
        val mapperOutput = graph.mapVertices[Double](mapper)
        mapperOutput.getVertices.print

        val mapperOutput2 = graph.mapVertices[Double]((vertex: Vertex[Long, Long]) => vertex.getValue.toDouble)
        mapperOutput2.getVertices.print
    }

    case class Key(key: Long)

    case class Value(value: Long)

    def runWithCases = {
        // GRAPH CREATION
        val ee = ExecutionEnvironment.createLocalEnvironment(2)
        val vertices = ee.fromElements[Vertex[Key, Value]](new Vertex(Key(1L), Value(1L)), new Vertex(Key(2L), Value(2L)))
        val edges = ee.fromElements[Edge[Key, Value]](new Edge(Key(1L), Key(2L), Value(3L)))
        val graph = Graph.fromDataSet[Key, Value, Value](vertices, edges, ee)

        val mapper = new CustomCaseMap
        val mapperOutput = graph.mapVertices[Value](mapper)
        mapperOutput.getVertices.print

        val mapperOutput2 = graph.mapVertices[Value]((vertex: Vertex[Key, Value]) => Value(vertex.getValue.value * 2))
        mapperOutput2.getVertices.print
    }


    class CustomMap extends MapFunction[Vertex[Long, Long], Double] {
        override def map(value: Vertex[Long, Long]): Double = value.getValue.toDouble
    }

    class CustomCaseMap extends MapFunction[Vertex[Key, Value], Value] {
        override def map(value: Vertex[Key, Value]): Value = Value(value.getValue.value * 2)
    }

}