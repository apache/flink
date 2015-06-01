package org.apache.flink.graph.scala

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{tuple => jtuple}
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

    def joinWithVertices[T: TypeInformation : ClassTag](inputDataset: DataSet[(K, T)], mapper: MapFunction[(VV, T), VV]): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[VV, T], VV]() {
            override def map(value: jtuple.Tuple2[VV, T]): VV = {
                mapper.map((value.f0, value.f1))
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithVertices[T](javaTupleSet, newmapper))
    }

    def joinWithVertices[T: TypeInformation](inputDataset: DataSet[(K, T)], fun: (VV, T) => VV): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[VV, T], VV]() {
            val cleanFun = clean(fun)

            override def map(value: jtuple.Tuple2[VV, T]): VV = {
                cleanFun(value.f0, value.f1)
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithVertices[T](javaTupleSet, newmapper))
    }

    def joinWithEdges[T: TypeInformation](inputDataset: DataSet[(K, K, T)], mapper: MapFunction[(EV, T), EV]): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                mapper.map((value.f0, value.f1))
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple3(scalatuple._1, scalatuple._2, scalatuple._3)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdges[T](javaTupleSet, newmapper))
    }

    def joinWithEdges[T: TypeInformation](inputDataset: DataSet[(K, K, T)], fun: (EV, T) => EV): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            val cleanFun = clean(fun)

            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                cleanFun(value.f0, value.f1)
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple3(scalatuple._1, scalatuple._2, scalatuple._3)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdges[T](javaTupleSet, newmapper))
    }

    def joinWithEdgesOnSource[T: TypeInformation](inputDataset: DataSet[(K, T)], mapper: MapFunction[(EV, T), EV]): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                mapper.map((value.f0, value.f1))
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdgesOnSource[T](javaTupleSet, newmapper))
    }

    def joinWithEdgesOnSource[T: TypeInformation](inputDataset: DataSet[(K, T)], fun: (EV, T) => EV): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            val cleanFun = clean(fun)

            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                cleanFun(value.f0, value.f1)
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdgesOnSource[T](javaTupleSet, newmapper))
    }

    def joinWithEdgesOnTarget[T: TypeInformation](inputDataset: DataSet[(K, T)], mapper: MapFunction[(EV, T), EV]): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                mapper.map((value.f0, value.f1))
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdgesOnTarget[T](javaTupleSet, newmapper))
    }

    def joinWithEdgesOnTarget[T: TypeInformation](inputDataset: DataSet[(K, T)], fun: (EV, T) => EV): Graph[K, VV, EV] = {
        val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
            val cleanFun = clean(fun)

            override def map(value: jtuple.Tuple2[EV, T]): EV = {
                cleanFun(value.f0, value.f1)
            }
        }
        val javaTupleSet = inputDataset.map(scalatuple => new jtuple.Tuple2(scalatuple._1, scalatuple._2)).javaSet
        new Graph[K, VV, EV](jgraph.joinWithEdgesOnTarget[T](javaTupleSet, newmapper))
    }


}