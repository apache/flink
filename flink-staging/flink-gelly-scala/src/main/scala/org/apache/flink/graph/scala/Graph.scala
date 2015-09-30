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
import org.apache.flink.graph._
import org.apache.flink.graph.validation.GraphValidator
import org.apache.flink.graph.gsa.{ApplyFunction, GSAConfiguration, GatherFunction, SumFunction}
import org.apache.flink.graph.spargel.{MessagingFunction, VertexCentricConfiguration, VertexUpdateFunction}
import org.apache.flink.{graph => jg}
import _root_.scala.collection.JavaConverters._
import _root_.scala.reflect.ClassTag
import org.apache.flink.types.NullValue

object Graph {

  /**
  * Creates a Graph from a DataSet of vertices and a DataSet of edges.
  */
  def fromDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](vertices: DataSet[Vertex[K, VV]], edges: DataSet[Edge[K, EV]],
                              env: ExecutionEnvironment): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromDataSet[K, VV, EV](vertices.javaSet, edges.javaSet, env.getJavaEnv))
  }

  /**
  * Creates a Graph from a DataSet of edges.
  * Vertices are created automatically and their values are set to NullValue.
  */
  def fromDataSet[K: TypeInformation : ClassTag, EV: TypeInformation : ClassTag]
  (edges: DataSet[Edge[K, EV]], env: ExecutionEnvironment): Graph[K, NullValue, EV] = {
    wrapGraph(jg.Graph.fromDataSet[K, EV](edges.javaSet, env.getJavaEnv))
  }

  /**
  * Creates a graph from a DataSet of edges.
  * Vertices are created automatically and their values are set by applying the provided
  * map function to the vertex ids.
  */
  def fromDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: DataSet[Edge[K, EV]], env: ExecutionEnvironment,
  mapper: MapFunction[K, VV]): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromDataSet[K, VV, EV](edges.javaSet, mapper, env.getJavaEnv))
  }

  /**
  * Creates a Graph from a Seq of vertices and a Seq of edges.
  */
  def fromCollection[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](vertices: Seq[Vertex[K, VV]], edges: Seq[Edge[K, EV]], env:
  ExecutionEnvironment): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromCollection[K, VV, EV](vertices.asJavaCollection, edges
      .asJavaCollection, env.getJavaEnv))
  }

  /**
  * Creates a Graph from a Seq of edges.
  * Vertices are created automatically and their values are set to NullValue.
  */
  def fromCollection[K: TypeInformation : ClassTag, EV: TypeInformation : ClassTag]
  (edges: Seq[Edge[K, EV]], env: ExecutionEnvironment): Graph[K, NullValue, EV] = {
    wrapGraph(jg.Graph.fromCollection[K, EV](edges.asJavaCollection, env.getJavaEnv))
  }

  /**
  * Creates a graph from a Seq of edges.
  * Vertices are created automatically and their values are set by applying the provided
  * map function to the vertex ids.
  */
  def fromCollection[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: Seq[Edge[K, EV]], env: ExecutionEnvironment,
  mapper: MapFunction[K, VV]): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromCollection[K, VV, EV](edges.asJavaCollection, mapper, env.getJavaEnv))
  }

  /**
  * Creates a Graph from a DataSets of Tuples.
  */
  def fromTupleDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](vertices: DataSet[(K, VV)], edges: DataSet[(K, K, EV)],
                              env: ExecutionEnvironment): Graph[K, VV, EV] = {
    val javaTupleVertices = vertices.map(v => new jtuple.Tuple2(v._1, v._2)).javaSet
    val javaTupleEdges = edges.map(v => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, VV, EV](javaTupleVertices, javaTupleEdges,
        env.getJavaEnv))
  }

  /**
  * Creates a Graph from a DataSet of Tuples representing the edges.
  * Vertices are created automatically and their values are set to NullValue.
  */
  def fromTupleDataSet[K: TypeInformation : ClassTag, EV: TypeInformation : ClassTag]
  (edges: DataSet[(K, K, EV)], env: ExecutionEnvironment): Graph[K, NullValue, EV] = {
    val javaTupleEdges = edges.map(v => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, EV](javaTupleEdges, env.getJavaEnv))
  }

  /**
  * Creates a Graph from a DataSet of Tuples representing the edges.
  * Vertices are created automatically and their values are set by applying the provided
  * map function to the vertex ids.
  */
  def fromTupleDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: DataSet[(K, K, EV)], env: ExecutionEnvironment,
  mapper: MapFunction[K, VV]): Graph[K, VV, EV] = {
    val javaTupleEdges = edges.map(v => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, VV, EV](javaTupleEdges, mapper, env.getJavaEnv))
  }

  /**
  * Creates a Graph with from a CSV file of vertices and a CSV file of edges
  * 
  * @param The Execution Environment.
  * @param pathEdges The file path containing the edges.
  * @param readVertices Defines whether the vertices have associated values.
  * If set to false, the vertex input is ignored and vertices are created from the edges file.
  * @param pathVertices The file path containing the vertices.
  * @param hasEdgeValues Defines whether the edges have associated values. True by default.
  * @param lineDelimiterVertices The string that separates lines in the vertices file.
  * It defaults to newline.
  * @param fieldDelimiterVertices The string that separates vertex Ids from vertex values
  * in the vertices file.
  * @param quoteCharacterVertices The character to use for quoted String parsing
  * in the vertices file. Disabled by default.
  * @param ignoreFirstLineVertices Whether the first line in the vertices file should be ignored.
  * @param ignoreCommentsVertices Lines that start with the given String in the vertices file
  * are ignored, disabled by default.
  * @param lenientVertices Whether the parser should silently ignore malformed lines in the
  * vertices file.
  * @param includedFieldsVertices The fields in the vertices file that should be read.
  * By default all fields are read.
  * @param lineDelimiterEdges The string that separates lines in the edges file.
  * It defaults to newline.
  * @param fieldDelimiterEdges The string that separates fields in the edges file.
  * @param quoteCharacterEdges The character to use for quoted String parsing
  * in the edges file. Disabled by default.
  * @param ignoreFirstLineEdges Whether the first line in the vertices file should be ignored.
  * @param ignoreCommentsEdges Lines that start with the given String in the edges file
  * are ignored, disabled by default.
  * @param lenientEdges Whether the parser should silently ignore malformed lines in the
  * edges file.
  * @param includedFieldsEdges The fields in the edges file that should be read.
  * By default all fields are read.
  * @param mapper If no vertex values are provided, this mapper can be used to initialize them.
  * 
  */
  // scalastyle:off
  // This method exceeds the max allowed number of parameters -->  
  def fromCsvReader[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag,
    EV: TypeInformation : ClassTag](
      env: ExecutionEnvironment,
      pathEdges: String,
      readVertices: Boolean,
      pathVertices: String = null,
      hasEdgeValues: Boolean = true,
      lineDelimiterVertices: String = "\n",
      fieldDelimiterVertices: String = ",",
      quoteCharacterVertices: Character = null,
      ignoreFirstLineVertices: Boolean = false,
      ignoreCommentsVertices: String = null,
      lenientVertices: Boolean = false,
      includedFieldsVertices: Array[Int] = null,
      lineDelimiterEdges: String = "\n",
      fieldDelimiterEdges: String = ",",
      quoteCharacterEdges: Character = null,
      ignoreFirstLineEdges: Boolean = false,
      ignoreCommentsEdges: String = null,
      lenientEdges: Boolean = false,
      includedFieldsEdges: Array[Int] = null,
      mapper: MapFunction[K, VV] = null) = {

    // with vertex and edge values
    if (readVertices && hasEdgeValues) {
      if (pathVertices.equals(null)) {
        throw new IllegalArgumentException(
            "The vertices file path must be specified when readVertices is true.")
      } else {
        val vertices = env.readCsvFile[(K, VV)](pathVertices, lineDelimiterVertices,
            fieldDelimiterVertices, quoteCharacterVertices, ignoreFirstLineVertices,
            ignoreCommentsVertices, lenientVertices, includedFieldsVertices)

        val edges = env.readCsvFile[(K, K, EV)](pathEdges, lineDelimiterEdges, fieldDelimiterEdges,
            quoteCharacterEdges, ignoreFirstLineEdges, ignoreCommentsEdges, lenientEdges,
            includedFieldsEdges)
     
        fromTupleDataSet[K, VV, EV](vertices, edges, env) 
      }
    }
    // with vertex value and no edge value
    else if (readVertices && (!hasEdgeValues)) {
       if (pathVertices.equals(null)) {
        throw new IllegalArgumentException(
            "The vertices file path must be specified when readVertices is true.")
      } else {
        val vertices = env.readCsvFile[(K, VV)](pathVertices, lineDelimiterVertices,
            fieldDelimiterVertices, quoteCharacterVertices, ignoreFirstLineVertices,
            ignoreCommentsVertices, lenientVertices, includedFieldsVertices)

        val edges = env.readCsvFile[(K, K)](pathEdges, lineDelimiterEdges, fieldDelimiterEdges,
            quoteCharacterEdges, ignoreFirstLineEdges, ignoreCommentsEdges, lenientEdges,
            includedFieldsEdges).map(edge => (edge._1, edge._2, NullValue.getInstance))

        fromTupleDataSet[K, VV, NullValue](vertices, edges, env)
      }
    }
    // with edge value and no vertex value
    else if ((!readVertices) && hasEdgeValues) {
      val edges = env.readCsvFile[(K, K, EV)](pathEdges, lineDelimiterEdges, fieldDelimiterEdges,
        quoteCharacterEdges, ignoreFirstLineEdges, ignoreCommentsEdges, lenientEdges,
        includedFieldsEdges)

      // initializer provided
      if (mapper != null) {
        fromTupleDataSet[K, VV, EV](edges, env, mapper)
      }
      else {
        fromTupleDataSet[K, EV](edges, env) 
      }
    }
    // with no edge value and no vertex value
    else {
      val edges = env.readCsvFile[(K, K)](pathEdges, lineDelimiterEdges, fieldDelimiterEdges,
      quoteCharacterEdges, ignoreFirstLineEdges, ignoreCommentsEdges,
      lenientEdges, includedFieldsEdges).map(edge => (edge._1, edge._2, NullValue.getInstance))

      // no initializer provided
      if (mapper != null) {
        fromTupleDataSet[K, VV, NullValue](edges, env, mapper)
      }
      else {
        fromTupleDataSet[K, NullValue](edges, env) 
      }
    }
  }
// scalastyle:on

}

/**
 * Represents a graph consisting of {@link Edge edges} and {@link Vertex vertices}.
 * @param jgraph the underlying java api Graph.
 * @tparam K the key type for vertex and edge identifiers
 * @tparam VV the value type for vertices
 * @tparam EV the value type for edges
 * @see org.apache.flink.graph.Edge
 * @see org.apache.flink.graph.Vertex
 */
final class Graph[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
TypeInformation : ClassTag](jgraph: jg.Graph[K, VV, EV]) {

  private[flink] def getWrappedGraph = jgraph


  private[flink] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    if (jgraph.getContext.getConfig.isClosureCleanerEnabled) {
      ClosureCleaner.clean(f, checkSerializable)
    }
    ClosureCleaner.ensureSerializable(f)
    f
  }

  /**
   * @return the vertex DataSet.
   */
  def getVertices = wrap(jgraph.getVertices)

  /**
   * @return the edge DataSet.
   */
  def getEdges = wrap(jgraph.getEdges)

  /**
   * @return the vertex DataSet as Tuple2.
   */
  def getVerticesAsTuple2(): DataSet[(K, VV)] = {
    wrap(jgraph.getVerticesAsTuple2).map(jtuple => (jtuple.f0, jtuple.f1))
  }

  /**
   * @return the edge DataSet as Tuple3.
   */
  def getEdgesAsTuple3(): DataSet[(K, K, EV)] = {
    wrap(jgraph.getEdgesAsTuple3).map(jtuple => (jtuple.f0, jtuple.f1, jtuple.f2))
  }

  /**
  * @return a DataSet of Triplets,
  * consisting of (srcVertexId, trgVertexId, srcVertexValue, trgVertexValue, edgeValue)
  */
  def getTriplets(): DataSet[Triplet[K, VV, EV]] = {
    wrap(jgraph.getTriplets())
  }

  /**
   * Apply a function to the attribute of each vertex in the graph.
   *
   * @param mapper the map function to apply.
   * @return a new graph
   */
  def mapVertices[NV: TypeInformation : ClassTag](mapper: MapFunction[Vertex[K, VV], NV]):
  Graph[K, NV, EV] = {
    new Graph[K, NV, EV](jgraph.mapVertices[NV](
      mapper,
      createTypeInformation[Vertex[K, NV]]
    ))
  }

  /**
   * Apply a function to the attribute of each vertex in the graph.
   *
   * @param fun the map function to apply.
   * @return a new graph
   */
  def mapVertices[NV: TypeInformation : ClassTag](fun: Vertex[K, VV] => NV): Graph[K, NV, EV] = {
    val mapper: MapFunction[Vertex[K, VV], NV] = new MapFunction[Vertex[K, VV], NV] {
      val cleanFun = clean(fun)

      def map(in: Vertex[K, VV]): NV = cleanFun(in)
    }
    new Graph[K, NV, EV](jgraph.mapVertices[NV](mapper, createTypeInformation[Vertex[K, NV]]))
  }

  /**
   * Apply a function to the attribute of each edge in the graph.
   *
   * @param mapper the map function to apply.
   * @return a new graph
   */
  def mapEdges[NV: TypeInformation : ClassTag](mapper: MapFunction[Edge[K, EV], NV]): Graph[K,
    VV, NV] = {
    new Graph[K, VV, NV](jgraph.mapEdges[NV](
      mapper,
      createTypeInformation[Edge[K, NV]]
    ))
  }

  /**
   * Apply a function to the attribute of each edge in the graph.
   *
   * @param fun the map function to apply.
   * @return a new graph
   */
  def mapEdges[NV: TypeInformation : ClassTag](fun: Edge[K, EV] => NV): Graph[K, VV, NV] = {
    val mapper: MapFunction[Edge[K, EV], NV] = new MapFunction[Edge[K, EV], NV] {
      val cleanFun = clean(fun)

      def map(in: Edge[K, EV]): NV = cleanFun(in)
    }
    new Graph[K, VV, NV](jgraph.mapEdges[NV](mapper, createTypeInformation[Edge[K, NV]]))
  }

  /**
   * Joins the vertex DataSet of this graph with an input DataSet and applies
   * a UDF on the resulted values.
   *
   * @param inputDataSet the DataSet to join with.
   * @param mapper the UDF map function to apply.
   * @return a new graph where the vertex values have been updated.
   */
  def joinWithVertices[T: TypeInformation](inputDataSet: DataSet[(K, T)], mapper: MapFunction[
    (VV, T), VV]): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[VV, T], VV]() {
      override def map(value: jtuple.Tuple2[VV, T]): VV = {
        mapper.map((value.f0, value.f1))
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithVertices[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the vertex DataSet of this graph with an input DataSet and applies
   * a UDF on the resulted values.
   *
   * @param inputDataSet the DataSet to join with.
   * @param fun the UDF map function to apply.
   * @return a new graph where the vertex values have been updated.
   */
  def joinWithVertices[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (VV, T) => VV):
  Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[VV, T], VV]() {
      val cleanFun = clean(fun)

      override def map(value: jtuple.Tuple2[VV, T]): VV = {
        cleanFun(value.f0, value.f1)
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithVertices[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on a composite key of both
   * source and target and applies a UDF on the resulted values.
   *
   * @param inputDataSet the DataSet to join with.
   * @param mapper the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdges[T: TypeInformation](inputDataSet: DataSet[(K, K, T)], mapper: MapFunction[
    (EV, T), EV]): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        mapper.map((value.f0, value.f1))
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple3(scalatuple._1,
      scalatuple._2, scalatuple._3)).javaSet
    wrapGraph(jgraph.joinWithEdges[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on a composite key of both
   * source and target and applies a UDF on the resulted values.
   *
   * @param inputDataSet the DataSet to join with.
   * @param fun the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdges[T: TypeInformation](inputDataSet: DataSet[(K, K, T)], fun: (EV, T) => EV):
  Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      val cleanFun = clean(fun)

      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        cleanFun(value.f0, value.f1)
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple3(scalatuple._1,
      scalatuple._2, scalatuple._3)).javaSet
    wrapGraph(jgraph.joinWithEdges[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the source key of the
   * edges and the first attribute of the input DataSet and applies a UDF on
   * the resulted values. In case the inputDataSet contains the same key more
   * than once, only the first value will be considered.
   *
   * @param inputDataSet the DataSet to join with.
   * @param mapper the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdgesOnSource[T: TypeInformation](inputDataSet: DataSet[(K, T)], mapper:
  MapFunction[(EV, T), EV]): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        mapper.map((value.f0, value.f1))
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnSource[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the source key of the
   * edges and the first attribute of the input DataSet and applies a UDF on
   * the resulted values. In case the inputDataSet contains the same key more
   * than once, only the first value will be considered.
   *
   * @param inputDataSet the DataSet to join with.
   * @param fun the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdgesOnSource[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (EV, T) =>
    EV): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      val cleanFun = clean(fun)

      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        cleanFun(value.f0, value.f1)
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnSource[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the target key of the
   * edges and the first attribute of the input DataSet and applies a UDF on
   * the resulted values. Should the inputDataSet contain the same key more
   * than once, only the first value will be considered.
   *
   * @param inputDataSet the DataSet to join with.
   * @param mapper the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdgesOnTarget[T: TypeInformation](inputDataSet: DataSet[(K, T)], mapper:
  MapFunction[(EV, T), EV]): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        mapper.map((value.f0, value.f1))
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnTarget[T](javaTupleSet, newmapper))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the target key of the
   * edges and the first attribute of the input DataSet and applies a UDF on
   * the resulted values. Should the inputDataSet contain the same key more
   * than once, only the first value will be considered.
   *
   * @param inputDataSet the DataSet to join with.
   * @param fun the UDF map function to apply.
   * @tparam T the return type
   * @return a new graph where the edge values have been updated.
   */
  def joinWithEdgesOnTarget[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (EV, T) =>
    EV): Graph[K, VV, EV] = {
    val newmapper = new MapFunction[jtuple.Tuple2[EV, T], EV]() {
      val cleanFun = clean(fun)

      override def map(value: jtuple.Tuple2[EV, T]): EV = {
        cleanFun(value.f0, value.f1)
      }
    }
    val javaTupleSet = inputDataSet.map(scalatuple => new jtuple.Tuple2(scalatuple._1,
      scalatuple._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnTarget[T](javaTupleSet, newmapper))
  }

  /**
   * Apply filtering functions to the graph and return a sub-graph that
   * satisfies the predicates for both vertices and edges.
   *
   * @param vertexFilter the filter function for vertices.
   * @param edgeFilter the filter function for edges.
   * @return the resulting sub-graph.
   */
  def subgraph(vertexFilter: FilterFunction[Vertex[K, VV]], edgeFilter: FilterFunction[Edge[K,
    EV]]) = {
    wrapGraph(jgraph.subgraph(vertexFilter, edgeFilter))
  }

  /**
   * Apply filtering functions to the graph and return a sub-graph that
   * satisfies the predicates for both vertices and edges.
   *
   * @param vertexFilterFun the filter function for vertices.
   * @param edgeFilterFun the filter function for edges.
   * @return the resulting sub-graph.
   */
  def subgraph(vertexFilterFun: Vertex[K, VV] => Boolean, edgeFilterFun: Edge[K, EV] =>
    Boolean) = {
    val vertexFilter = new FilterFunction[Vertex[K, VV]] {
      val cleanVertexFun = clean(vertexFilterFun)

      override def filter(value: Vertex[K, VV]): Boolean = cleanVertexFun(value)
    }

    val edgeFilter = new FilterFunction[Edge[K, EV]] {
      val cleanEdgeFun = clean(edgeFilterFun)

      override def filter(value: Edge[K, EV]): Boolean = cleanEdgeFun(value)
    }

    wrapGraph(jgraph.subgraph(vertexFilter, edgeFilter))
  }

  /**
   * Apply a filtering function to the graph and return a sub-graph that
   * satisfies the predicates only for the vertices.
   *
   * @param vertexFilter the filter function for vertices.
   * @return the resulting sub-graph.
   */
  def filterOnVertices(vertexFilter: FilterFunction[Vertex[K, VV]]) = {
    wrapGraph(jgraph.filterOnVertices(vertexFilter))
  }

  /**
   * Apply a filtering function to the graph and return a sub-graph that
   * satisfies the predicates only for the vertices.
   *
   * @param vertexFilterFun the filter function for vertices.
   * @return the resulting sub-graph.
   */
  def filterOnVertices(vertexFilterFun: Vertex[K, VV] => Boolean) = {
    val vertexFilter = new FilterFunction[Vertex[K, VV]] {
      val cleanVertexFun = clean(vertexFilterFun)

      override def filter(value: Vertex[K, VV]): Boolean = cleanVertexFun(value)
    }

    wrapGraph(jgraph.filterOnVertices(vertexFilter))
  }

  /**
   * Apply a filtering function to the graph and return a sub-graph that
   * satisfies the predicates only for the edges.
   *
   * @param edgeFilter the filter function for edges.
   * @return the resulting sub-graph.
   */
  def filterOnEdges(edgeFilter: FilterFunction[Edge[K, EV]]) = {
    wrapGraph(jgraph.filterOnEdges(edgeFilter))
  }

  /**
   * Apply a filtering function to the graph and return a sub-graph that
   * satisfies the predicates only for the edges.
   *
   * @param edgeFilterFun the filter function for edges.
   * @return the resulting sub-graph.
   */
  def filterOnEdges(edgeFilterFun: Edge[K, EV] => Boolean) = {
    val edgeFilter = new FilterFunction[Edge[K, EV]] {
      val cleanEdgeFun = clean(edgeFilterFun)

      override def filter(value: Edge[K, EV]): Boolean = cleanEdgeFun(value)
    }

    wrapGraph(jgraph.filterOnEdges(edgeFilter))
  }

  /**
   * Return the in-degree of all vertices in the graph
   *
   * @return A DataSet of Tuple2<vertexId, inDegree>
   */
  def inDegrees(): DataSet[(K, Long)] = {
    wrap(jgraph.inDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
  }

  /**
   * Return the out-degree of all vertices in the graph
   *
   * @return A DataSet of Tuple2<vertexId, outDegree>
   */
  def outDegrees(): DataSet[(K, Long)] = {
    wrap(jgraph.outDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
  }

  /**
   * Return the degree of all vertices in the graph
   *
   * @return A DataSet of Tuple2<vertexId, degree>
   */
  def getDegrees(): DataSet[(K, Long)] = {
    wrap(jgraph.getDegrees).map(javatuple => (javatuple.f0, javatuple.f1))
  }

  /**
   * This operation adds all inverse-direction edges to the graph.
   *
   * @return the undirected graph.
   */
  def getUndirected(): Graph[K, VV, EV] = {
    new Graph(jgraph.getUndirected)
  }

  /**
   * Reverse the direction of the edges in the graph
   *
   * @return a new graph with all edges reversed
   * @throws UnsupportedOperationException
   */
  def reverse(): Graph[K, VV, EV] = {
    new Graph(jgraph.reverse())
  }

  /**
   * Compute an aggregate over the edges of each vertex. The function applied
   * on the edges has access to the vertex value.
   *
   * @param edgesFunction the function to apply to the neighborhood
   * @param direction     the edge direction (in-, out-, all-)
   * @tparam T           the output type
   * @return a dataset of a T
   */
  def groupReduceOnEdges[T: TypeInformation : ClassTag](edgesFunction:
                                                        EdgesFunctionWithVertexValue[K, VV, EV,
                                                          T], direction: EdgeDirection):
  DataSet[T] = {
    wrap(jgraph.groupReduceOnEdges(edgesFunction, direction, createTypeInformation[T]))
  }

  /**
   * Compute an aggregate over the edges of each vertex. The function applied
   * on the edges has access to the vertex value.
   *
   * @param edgesFunction the function to apply to the neighborhood
   * @param direction     the edge direction (in-, out-, all-)
   * @tparam T           the output type
   * @return a dataset of a T
   */
  def groupReduceOnEdges[T: TypeInformation : ClassTag](edgesFunction: EdgesFunction[K, EV, T],
                                                        direction: EdgeDirection): DataSet[T] = {
    wrap(jgraph.groupReduceOnEdges(edgesFunction, direction, createTypeInformation[T]))
  }

  /**
   * Compute an aggregate over the neighbors (edges and vertices) of each
   * vertex. The function applied on the neighbors has access to the vertex
   * value.
   *
   * @param neighborsFunction the function to apply to the neighborhood
   * @param direction         the edge direction (in-, out-, all-)
   * @tparam T               the output type
   * @return a dataset of a T
   */
  def groupReduceOnNeighbors[T: TypeInformation : ClassTag](neighborsFunction:
                                                            NeighborsFunctionWithVertexValue[K,
                                                              VV, EV, T], direction:
                                                            EdgeDirection): DataSet[T] = {
    wrap(jgraph.groupReduceOnNeighbors(neighborsFunction, direction, createTypeInformation[T]))
  }

  /**
   * Compute an aggregate over the neighbors (edges and vertices) of each
   * vertex.
   *
   * @param neighborsFunction the function to apply to the neighborhood
   * @param direction         the edge direction (in-, out-, all-)
   * @tparam T               the output type
   * @return a dataset of a T
   */
  def groupReduceOnNeighbors[T: TypeInformation : ClassTag](neighborsFunction:
                                                            NeighborsFunction[K, VV, EV, T],
                                                            direction: EdgeDirection):
  DataSet[T] = {
    wrap(jgraph.groupReduceOnNeighbors(neighborsFunction, direction, createTypeInformation[T]))
  }

  /**
   * @return a long integer representing the number of vertices
   */
  def numberOfVertices(): Long = {
    jgraph.numberOfVertices()
  }

  /**
   * @return a long integer representing the number of edges
   */
  def numberOfEdges(): Long = {
    jgraph.numberOfEdges()
  }

  /**
   * @return The IDs of the vertices as DataSet
   */
  def getVertexIds(): DataSet[K] = {
    wrap(jgraph.getVertexIds)
  }

  /**
   * @return The IDs of the edges as DataSet
   */
  def getEdgeIds(): DataSet[(K, K)] = {
    wrap(jgraph.getEdgeIds).map(jtuple => (jtuple.f0, jtuple.f1))
  }

  /**
   * Adds the input vertex to the graph. If the vertex already
   * exists in the graph, it will not be added again.
   *
   * @param vertex the vertex to be added
   * @return the new graph containing the existing vertices as well as the one just added
   */
  def addVertex(vertex: Vertex[K, VV]) = {
    wrapGraph(jgraph.addVertex(vertex))
  }

  /**
  * Adds the list of vertices, passed as input, to the graph.
  * If the vertices already exist in the graph, they will not be added once more.
  *
  * @param verticesToAdd the list of vertices to add
  * @return the new graph containing the existing and newly added vertices
  */
  def addVertices(vertices: List[Vertex[K, VV]]): Graph[K, VV, EV] = {
    wrapGraph(jgraph.addVertices(vertices.asJava))
  }

  /**
  * Adds the given list edges to the graph.
  *
  * When adding an edge for a non-existing set of vertices,
  * the edge is considered invalid and ignored.
  *
  * @param newEdges the data set of edges to be added
  * @return a new graph containing the existing edges plus the newly added edges.
  */
  def addEdges(edges: List[Edge[K, EV]]): Graph[K, VV, EV] = {
    wrapGraph(jgraph.addEdges(edges.asJava))
  }

    /**
   * Adds the given edge to the graph. If the source and target vertices do
   * not exist in the graph, they will also be added.
   *
   * @param source the source vertex of the edge
   * @param target the target vertex of the edge
   * @param edgeValue the edge value
   * @return the new graph containing the existing vertices and edges plus the
   *         newly added edge
   */
  def addEdge(source: Vertex[K, VV], target: Vertex[K, VV], edgeValue: EV) = {
    wrapGraph(jgraph.addEdge(source, target, edgeValue))
  }

  /**
   * Removes the given vertex and its edges from the graph.
   *
   * @param vertex the vertex to remove
   * @return the new graph containing the existing vertices and edges without
   *         the removed vertex and its edges
   */
  def removeVertex(vertex: Vertex[K, VV]) = {
    wrapGraph(jgraph.removeVertex(vertex))
  }

    /**
   * Removes the given vertex and its edges from the graph.
   *
   * @param vertex the vertex to remove
   * @return the new graph containing the existing vertices and edges without
   *         the removed vertex and its edges
   */
  def removeVertices(vertices: List[Vertex[K, VV]]): Graph[K, VV, EV] = {
    wrapGraph(jgraph.removeVertices(vertices.asJava))
  }

  /**
   * Removes all edges that match the given edge from the graph.
   *
   * @param edge the edge to remove
   * @return the new graph containing the existing vertices and edges without
   *         the removed edges
   */
  def removeEdge(edge: Edge[K, EV]) = {
    wrapGraph(jgraph.removeEdge(edge))
  }

  /**
   * Removes all the edges that match the edges in the given data set from the graph.
   *
   * @param edgesToBeRemoved the list of edges to be removed
   * @return a new graph where the edges have been removed and in which the vertices remained intact
   */
  def removeEdges(edges: List[Edge[K, EV]]): Graph[K, VV, EV] = {
    wrapGraph(jgraph.removeEdges(edges.asJava))
  }

  /**
   * Performs union on the vertices and edges sets of the input graphs
   * removing duplicate vertices but maintaining duplicate edges.
   *
   * @param graph the graph to perform union with
   * @return a new graph
   */
  def union(graph: Graph[K, VV, EV]) = {
    wrapGraph(jgraph.union(graph.getWrappedGraph))
  }

  /**
  * Performs Difference on the vertex and edge sets of the input graphs
  * removes common vertices and edges. If a source/target vertex is removed,
  * its corresponding edge will also be removed
  * @param graph the graph to perform difference with
  * @return a new graph where the common vertices and edges have been removed
  */
  def difference(graph: Graph[K, VV, EV]) = {
    wrapGraph(jgraph.difference(graph.getWrappedGraph))
  }

  /**
   * Compute an aggregate over the neighbor values of each
   * vertex.
   *
   * @param reduceNeighborsFunction the function to apply to the neighborhood
   * @param direction               the edge direction (in-, out-, all-)
   * @return a Dataset containing one value per vertex (vertex id, aggregate vertex value)
   */
  def reduceOnNeighbors(reduceNeighborsFunction: ReduceNeighborsFunction[VV], direction:
  EdgeDirection): DataSet[(K, VV)] = {
    wrap(jgraph.reduceOnNeighbors(reduceNeighborsFunction, direction)).map(jtuple => (jtuple
      .f0, jtuple.f1))
  }

  /**
   * Compute an aggregate over the edge values of each vertex.
   *
   * @param reduceEdgesFunction the function to apply to the neighborhood
   * @param direction           the edge direction (in-, out-, all-)
   * @return a Dataset containing one value per vertex(vertex key, aggegate edge value)
   * @throws IllegalArgumentException
   */
  def reduceOnEdges(reduceEdgesFunction: ReduceEdgesFunction[EV], direction: EdgeDirection):
  DataSet[(K, EV)] = {
    wrap(jgraph.reduceOnEdges(reduceEdgesFunction, direction)).map(jtuple => (jtuple.f0,
      jtuple.f1))
  }

  def run[T: TypeInformation : ClassTag](algorithm: GraphAlgorithm[K, VV, EV, T]):
  T = {
    jgraph.run(algorithm)
  }

  /**
   * Runs a Vertex-Centric iteration on the graph.
   * No configuration options are provided.
   *
   * @param vertexUpdateFunction the vertex update function
   * @param messagingFunction the messaging function
   * @param maxIterations maximum number of iterations to perform
   *
   * @return the updated Graph after the vertex-centric iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runVertexCentricIteration[M](vertexUpdateFunction: VertexUpdateFunction[K, VV, M],
                                   messagingFunction: MessagingFunction[K, VV, M, EV],
                                   maxIterations: Int): Graph[K, VV, EV] = {
    wrapGraph(jgraph.runVertexCentricIteration(vertexUpdateFunction, messagingFunction,
      maxIterations))
  }

  /**
   * Runs a Vertex-Centric iteration on the graph with configuration options.
   *
   * @param vertexUpdateFunction the vertex update function
   * @param messagingFunction the messaging function
   * @param maxIterations maximum number of iterations to perform
   * @param parameters the iteration configuration parameters
   *
   * @return the updated Graph after the vertex-centric iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runVertexCentricIteration[M](vertexUpdateFunction: VertexUpdateFunction[K, VV, M],
                                   messagingFunction: MessagingFunction[K, VV, M, EV],
                                   maxIterations: Int, parameters: VertexCentricConfiguration):
  Graph[K, VV, EV] = {
    wrapGraph(jgraph.runVertexCentricIteration(vertexUpdateFunction, messagingFunction,
      maxIterations, parameters))
  }

  /**
   * Runs a Gather-Sum-Apply iteration on the graph.
   * No configuration options are provided.
   *
   * @param gatherFunction the gather function collects information about adjacent
   *                       vertices and edges
   * @param sumFunction the sum function aggregates the gathered information
   * @param applyFunction the apply function updates the vertex values with the aggregates
   * @param maxIterations maximum number of iterations to perform
   * @tparam M the intermediate type used between gather, sum and apply
   *
   * @return the updated Graph after the gather-sum-apply iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runGatherSumApplyIteration[M](gatherFunction: GatherFunction[VV, EV, M], sumFunction:
  SumFunction[VV, EV, M], applyFunction: ApplyFunction[K, VV, M], maxIterations: Int): Graph[K,
    VV, EV] = {
    wrapGraph(jgraph.runGatherSumApplyIteration(gatherFunction, sumFunction, applyFunction,
      maxIterations))
  }

  /**
   * Runs a Gather-Sum-Apply iteration on the graph with configuration options.
   *
   * @param gatherFunction the gather function collects information about adjacent
   *                       vertices and edges
   * @param sumFunction the sum function aggregates the gathered information
   * @param applyFunction the apply function updates the vertex values with the aggregates
   * @param maxIterations maximum number of iterations to perform
   * @param parameters the iteration configuration parameters
   * @tparam M the intermediate type used between gather, sum and apply
   *
   * @return the updated Graph after the gather-sum-apply iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runGatherSumApplyIteration[M](gatherFunction: GatherFunction[VV, EV, M], sumFunction:
  SumFunction[VV, EV, M], applyFunction: ApplyFunction[K, VV, M], maxIterations: Int,
                                    parameters: GSAConfiguration): Graph[K, VV, EV] = {
    wrapGraph(jgraph.runGatherSumApplyIteration(gatherFunction, sumFunction, applyFunction,
      maxIterations, parameters))
  }

  def validate(validator: GraphValidator[K, VV, EV]): Boolean = {
    jgraph.validate(validator)
  }

}
