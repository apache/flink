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

package org.apache.flink.graph.scala

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{tuple => jtuple}
import org.apache.flink.api.scala._
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.{graph => jg}


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

    def mapVertices[NV: TypeInformation](mapper: MapFunction[Vertex[K, VV], NV]): Graph[K, NV, EV] = {
        new Graph[K, NV, EV](jgraph.mapVertices[NV](
            mapper,
            createTypeInformation[Vertex[K, NV]]
        ))
    }

    def mapVertices[NV: TypeInformation](fun: Vertex[K, VV] => NV): Graph[K, NV, EV] = {
        val mapper: MapFunction[Vertex[K, VV], NV] = new MapFunction[Vertex[K, VV], NV] {
            val cleanFun = clean(fun)

            def map(in: Vertex[K, VV]): NV = cleanFun(in)
        }
        new Graph[K, NV, EV](jgraph.mapVertices[NV](mapper, createTypeInformation[Vertex[K, NV]]))
    }

    def mapEdges[NV: TypeInformation](mapper: MapFunction[Edge[K, EV], NV]): Graph[K, VV, NV] = {
        new Graph[K, VV, NV](jgraph.mapEdges[NV](
            mapper,
            createTypeInformation[Edge[K, NV]]
        ))
    }

    def mapEdges[NV: TypeInformation](fun: Edge[K, EV] => NV): Graph[K, VV, NV] = {
        val mapper: MapFunction[Edge[K, EV], NV] = new MapFunction[Edge[K, EV], NV] {
            val cleanFun = clean(fun)

            def map(in: Edge[K, EV]): NV = cleanFun(in)
        }
        new Graph[K, VV, NV](jgraph.mapEdges[NV](mapper, createTypeInformation[Edge[K, NV]]))
    }

    def joinWithVertices[T: TypeInformation](inputDataset: DataSet[(K, T)], mapper: MapFunction[(VV, T), VV]): Graph[K, VV, EV] = {
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

    def subgraph(vertexFilter: FilterFunction[Vertex[K, VV]], edgeFilter: FilterFunction[Edge[K, EV]]) = {
        new Graph[K, VV, EV](jgraph.subgraph(vertexFilter, edgeFilter))
    }

    def subgraph(vertexFilterFun: Vertex[K, VV] => Boolean, edgeFilterFun: Edge[K, EV] => Boolean) = {
        val vertexFilter = new FilterFunction[Vertex[K, VV]] {
            val cleanVertexFun = clean(vertexFilterFun)

            override def filter(value: Vertex[K, VV]): Boolean = cleanVertexFun(value)
        }

        val edgeFilter = new FilterFunction[Edge[K, EV]] {
            val cleanEdgeFun = clean(edgeFilterFun)

            override def filter(value: Edge[K, EV]): Boolean = cleanEdgeFun(value)
        }

        new Graph[K, VV, EV](jgraph.subgraph(vertexFilter, edgeFilter))
    }

    def filterOnVertices(vertexFilter: FilterFunction[Vertex[K, VV]]) = {
        new Graph[K, VV, EV](jgraph.filterOnVertices(vertexFilter))
    }

    def filterOnVertices(vertexFilterFun: Vertex[K, VV] => Boolean) = {
        val vertexFilter = new FilterFunction[Vertex[K, VV]] {
            val cleanVertexFun = clean(vertexFilterFun)

            override def filter(value: Vertex[K, VV]): Boolean = cleanVertexFun(value)
        }

        new Graph[K, VV, EV](jgraph.filterOnVertices(vertexFilter))
    }

    def filterOnEdges(edgeFilter: FilterFunction[Edge[K, EV]]) = {
        new Graph[K, VV, EV](jgraph.filterOnEdges(edgeFilter))
    }

    def filterOnEdges(edgeFilterFun: Edge[K, EV] => Boolean) = {
        val edgeFilter = new FilterFunction[Edge[K, EV]] {
            val cleanEdgeFun = clean(edgeFilterFun)

            override def filter(value: Edge[K, EV]): Boolean = cleanEdgeFun(value)
        }

        new Graph[K, VV, EV](jgraph.filterOnEdges(edgeFilter))
    }

    def inDegrees(): DataSet[(K, Long)] = {
       wrap(jgraph.inDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
    }

    def outDegrees(): DataSet[(K, Long)] = {
        wrap(jgraph.outDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
    }

    def getDegrees(): DataSet[(K, Long)] = {
        wrap(jgraph.getDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
    }

}