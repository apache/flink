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

import org.apache.flink.api.common.functions.MapFunction
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
}