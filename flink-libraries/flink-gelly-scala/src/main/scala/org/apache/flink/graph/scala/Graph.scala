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
import org.apache.flink.graph.asm.translate.TranslateFunction
import org.apache.flink.graph.gsa.{ApplyFunction, GSAConfiguration, SumFunction, GatherFunction => GSAGatherFunction}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, VertexCentricConfiguration}
import org.apache.flink.graph.spargel.{ScatterFunction, ScatterGatherConfiguration, GatherFunction => SpargelGatherFunction}
import org.apache.flink.graph.validation.GraphValidator
import org.apache.flink.types.{LongValue, NullValue}
import org.apache.flink.util.Preconditions
import org.apache.flink.{graph => jg}

import _root_.scala.collection.JavaConverters._
import _root_.scala.reflect.ClassTag

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
   * vertexValueInitializer map function to the vertex ids.
   */
  def fromDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: DataSet[Edge[K, EV]],
  vertexValueInitializer: MapFunction[K, VV], env: ExecutionEnvironment): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromDataSet[K, VV, EV](edges.javaSet, vertexValueInitializer,
      env.getJavaEnv))
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
   * vertexValueInitializer map function to the vertex ids.
   */
  def fromCollection[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: Seq[Edge[K, EV]], vertexValueInitializer: MapFunction[K, VV],
  env: ExecutionEnvironment): Graph[K, VV, EV] = {
    wrapGraph(jg.Graph.fromCollection[K, VV, EV](edges.asJavaCollection, vertexValueInitializer,
      env.getJavaEnv))
  }

  /**
   * Creates a graph from DataSets of tuples for vertices and for edges.
   * The first field of the Tuple2 vertex object will become the vertex ID
   * and the second field will become the vertex value.
   * The first field of the Tuple3 object for edges will become the source ID,
   * the second field will become the target ID, and the third field will become
   * the edge value. 
   */
  def fromTupleDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](vertices: DataSet[(K, VV)], edges: DataSet[(K, K, EV)],
                              env: ExecutionEnvironment): Graph[K, VV, EV] = {
    val javaTupleVertices = vertices.map((v: (K, VV)) => new jtuple.Tuple2(v._1, v._2)).javaSet
    val javaTupleEdges = edges.map((v: (K, K, EV)) => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, VV, EV](javaTupleVertices, javaTupleEdges,
      env.getJavaEnv))
  }

  /**
   * Creates a Graph from a DataSet of Tuples representing the edges.
   * The first field of the Tuple3 object for edges will become the source ID,
   * the second field will become the target ID, and the third field will become
   * the edge value.
   * Vertices are created automatically and their values are set to NullValue.
   */
  def fromTupleDataSet[K: TypeInformation : ClassTag, EV: TypeInformation : ClassTag]
  (edges: DataSet[(K, K, EV)], env: ExecutionEnvironment): Graph[K, NullValue, EV] = {
    val javaTupleEdges = edges.map((v: (K, K, EV)) => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, EV](javaTupleEdges, env.getJavaEnv))
  }

  /**
   * Creates a Graph from a DataSet of Tuples representing the edges.
   * The first field of the Tuple3 object for edges will become the source ID,
   * the second field will become the target ID, and the third field will become
   * the edge value.
   * Vertices are created automatically and their values are set by applying the provided
   * vertexValueInitializer map function to the vertex ids.
   */
  def fromTupleDataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag, EV:
  TypeInformation : ClassTag](edges: DataSet[(K, K, EV)],
  vertexValueInitializer: MapFunction[K, VV], env: ExecutionEnvironment): Graph[K, VV, EV] = {
    val javaTupleEdges = edges.map((v: (K, K, EV)) => new jtuple.Tuple3(v._1, v._2, v._3)).javaSet
    wrapGraph(jg.Graph.fromTupleDataSet[K, VV, EV](javaTupleEdges, vertexValueInitializer,
      env.getJavaEnv))
  }

  /**
   * Creates a Graph from a DataSet of Tuple2's representing the edges.
   * The first field of the Tuple2 object for edges will become the source ID,
   * the second field will become the target ID. The edge value will be set to NullValue.
   * Vertices are created automatically and their values are set to NullValue.
   */
  def fromTuple2DataSet[K: TypeInformation : ClassTag](edges: DataSet[(K, K)],
  env: ExecutionEnvironment): Graph[K, NullValue, NullValue] = {
    val javaTupleEdges = edges.map((v: (K, K)) => new jtuple.Tuple2(v._1, v._2)).javaSet
    wrapGraph(jg.Graph.fromTuple2DataSet[K](javaTupleEdges, env.getJavaEnv))
  }

  /**
   * Creates a Graph from a DataSet of Tuple2's representing the edges.
   * The first field of the Tuple2 object for edges will become the source ID,
   * the second field will become the target ID. The edge value will be set to NullValue.
   * Vertices are created automatically and their values are set by applying the provided
   * vertexValueInitializer map function to the vertex IDs.
   */
  def fromTuple2DataSet[K: TypeInformation : ClassTag, VV: TypeInformation : ClassTag]
  (edges: DataSet[(K, K)], vertexValueInitializer: MapFunction[K, VV],
  env: ExecutionEnvironment): Graph[K, VV, NullValue] = {
    val javaTupleEdges = edges.map((v: (K, K)) => new jtuple.Tuple2(v._1, v._2)).javaSet
    wrapGraph(jg.Graph.fromTuple2DataSet[K, VV](javaTupleEdges, vertexValueInitializer,
      env.getJavaEnv))
  }

  /** Creates a Graph from a CSV file of edges.
   *
   * The edge value is read from the CSV file if [[EV]] is not of type [[NullValue]]. Otherwise the
   * edge value is set to [[NullValue]].
   *
   * If the vertex value type [[VV]] is specified (unequal [[NullValue]]), then the vertex values
   * are read from the file specified by pathVertices. If the path has not been specified then the
   * vertexValueInitializer is used to initialize the vertex values of the vertices extracted from
   * the set of edges. If the vertexValueInitializer has not been set either, then the method
   * fails.
   *
   * @param env The Execution Environment.
   * @param pathEdges The file path containing the edges.
   * @param pathVertices The file path containing the vertices.
   * @param lineDelimiterVertices The string that separates lines in the vertices file. It defaults
   *                              to newline.
   * @param fieldDelimiterVertices The string that separates vertex Ids from vertex values in the
   *                               vertices file.
   * @param quoteCharacterVertices The character to use for quoted String parsing in the vertices
   *                               file. Disabled by default.
   * @param ignoreFirstLineVertices Whether the first line in the vertices file should be ignored.
   * @param ignoreCommentsVertices Lines that start with the given String in the vertices file
   *                               are ignored, disabled by default.
   * @param lenientVertices Whether the parser should silently ignore malformed lines in the
   *                        vertices file.
   * @param includedFieldsVertices The fields in the vertices file that should be read. By default
   *                               all fields are read.
   * @param lineDelimiterEdges The string that separates lines in the edges file. It defaults to
   *                           newline.
   * @param fieldDelimiterEdges The string that separates fields in the edges file.
   * @param quoteCharacterEdges The character to use for quoted String parsing in the edges file.
   *                            Disabled by default.
   * @param ignoreFirstLineEdges Whether the first line in the vertices file should be ignored.
   * @param ignoreCommentsEdges Lines that start with the given String in the edges file are
   *                            ignored, disabled by default.
   * @param lenientEdges Whether the parser should silently ignore malformed lines in the edges
   *                     file.
   * @param includedFieldsEdges The fields in the edges file that should be read. By default all
   *                            fields are read.
   * @param vertexValueInitializer  If no vertex values are provided, this mapper can be used to
   *                                initialize them, by applying a map transformation on the vertex
   *                                IDs.
   * @tparam K Vertex key type
   * @tparam VV Vertex value type
   * @tparam EV Edge value type
   * @return Graph with vertices and edges read from the given files.
   */
  // scalastyle:off
  // This method exceeds the max allowed number of parameters -->  
  def fromCsvReader[
      K: TypeInformation : ClassTag,
      VV: TypeInformation : ClassTag,
      EV: TypeInformation : ClassTag](
      env: ExecutionEnvironment,
      pathEdges: String,
      pathVertices: String = null,
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
      vertexValueInitializer: MapFunction[K, VV] = null)
    : Graph[K, VV, EV] = {

    Preconditions.checkNotNull(pathEdges)

    val evClassTag = implicitly[ClassTag[EV]]
    val vvClassTag = implicitly[ClassTag[VV]]

    val edges = if (evClassTag.runtimeClass.equals(classOf[NullValue])) {
      env.readCsvFile[(K, K)](
        pathEdges,
        lineDelimiterEdges,
        fieldDelimiterEdges,
        quoteCharacterEdges,
        ignoreFirstLineEdges,
        ignoreCommentsEdges,
        lenientEdges,
        includedFieldsEdges)
        .map((edge: (K, K)) => (edge._1, edge._2, NullValue.getInstance))
        .asInstanceOf[DataSet[(K, K, EV)]]
    } else {
      env.readCsvFile[(K, K, EV)](
        pathEdges,
        lineDelimiterEdges,
        fieldDelimiterEdges,
        quoteCharacterEdges,
        ignoreFirstLineEdges,
        ignoreCommentsEdges,
        lenientEdges,
        includedFieldsEdges)
    }

    if (vvClassTag.runtimeClass.equals(classOf[NullValue])) {
      fromTupleDataSet[K, EV](edges, env).asInstanceOf[Graph[K, VV, EV]]
    } else {
      if (pathVertices != null) {
        val vertices = env.readCsvFile[(K, VV)](pathVertices, lineDelimiterVertices,
          fieldDelimiterVertices, quoteCharacterVertices, ignoreFirstLineVertices,
          ignoreCommentsVertices, lenientVertices, includedFieldsVertices)

        fromTupleDataSet[K, VV, EV](vertices, edges, env)
      } else if (vertexValueInitializer != null) {
        fromTupleDataSet[K, VV, EV](edges, vertexValueInitializer, env)
      } else {
        throw new IllegalArgumentException("Path vertices path and vertex value initialzier must" +
          "not be null if the vertex value type is not NullValue.")
      }
    }
  }
  // scalastyle:on
}

/**
 * Represents a graph consisting of [[Edge]] edges and [[Vertex]] vertices.
 *
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
    wrap(jgraph.getVerticesAsTuple2).map(
      (v: jtuple.Tuple2[K, VV]) => (v.f0, v.f1))
  }

  /**
   * @return the edge DataSet as Tuple3.
   */
  def getEdgesAsTuple3(): DataSet[(K, K, EV)] = {
    wrap(jgraph.getEdgesAsTuple3).map(
      (e: jtuple.Tuple3[K, K, EV]) =>
        (e.f0, e.f1, e.f2))
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
   * Translate vertex and edge IDs using the given MapFunction.
   *
   * @param translator implements conversion from K to NEW
   * @return graph with translated vertex and edge IDs
   */
  def translateGraphIds[NEW: TypeInformation : ClassTag](translator: TranslateFunction[K, NEW]):
  Graph[NEW, VV, EV] = {
    new Graph[NEW, VV, EV](jgraph.translateGraphIds(translator))
  }

  /**
   * Translate vertex and edge IDs using the given function.
   *
   * @param fun implements conversion from K to NEW
   * @return graph with translated vertex and edge IDs
   */
  def translateGraphIds[NEW: TypeInformation : ClassTag](fun: (K, NEW) => NEW):
  Graph[NEW, VV, EV] = {
    val translator: TranslateFunction[K, NEW] = new TranslateFunction[K, NEW] {
      val cleanFun = clean(fun)

      def translate(in: K, reuse: NEW): NEW = cleanFun(in, reuse)
    }

    new Graph[NEW, VV, EV](jgraph.translateGraphIds(translator))
  }

  /**
   * Translate vertex values using the given MapFunction.
   *
   * @param translator implements conversion from VV to NEW
   * @return graph with translated vertex values
   */
  def translateVertexValues[NEW: TypeInformation : ClassTag](translator:
  TranslateFunction[VV, NEW]): Graph[K, NEW, EV] = {
    new Graph[K, NEW, EV](jgraph.translateVertexValues(translator))
  }

  /**
   * Translate vertex values using the given function.
   *
   * @param fun implements conversion from VV to NEW
   * @return graph with translated vertex values
   */
  def translateVertexValues[NEW: TypeInformation : ClassTag](fun: (VV, NEW) => NEW):
  Graph[K, NEW, EV] = {
    val translator: TranslateFunction[VV, NEW] = new TranslateFunction[VV, NEW] {
      val cleanFun = clean(fun)

      def translate(in: VV, reuse: NEW): NEW = cleanFun(in, reuse)
    }

    new Graph[K, NEW, EV](jgraph.translateVertexValues(translator))
  }

  /**
   * Translate edge values using the given MapFunction.
   *
   * @param translator implements conversion from EV to NEW
   * @return graph with translated edge values
   */
  def translateEdgeValues[NEW: TypeInformation : ClassTag](translator: TranslateFunction[EV, NEW]):
  Graph[K, VV, NEW] = {
    new Graph[K, VV, NEW](jgraph.translateEdgeValues(translator))
  }

  /**
   * Translate edge values using the given function.
   *
   * @param fun implements conversion from EV to NEW
   * @return graph with translated edge values
   */
  def translateEdgeValues[NEW: TypeInformation : ClassTag](fun: (EV, NEW) => NEW):
  Graph[K, VV, NEW] = {
    val translator: TranslateFunction[EV, NEW] = new TranslateFunction[EV, NEW] {
      val cleanFun = clean(fun)

      def translate(in: EV, reuse: NEW): NEW = cleanFun(in, reuse)
    }

    new Graph[K, VV, NEW](jgraph.translateEdgeValues(translator))
  }

  /**
   * Joins the vertex DataSet of this graph with an input Tuple2 DataSet and applies
   * a user-defined transformation on the values of the matched records.
   * The vertex ID and the first field of the Tuple2 DataSet are used as the join keys.
   * 
   * @param inputDataSet the Tuple2 DataSet to join with.
   * The first field of the Tuple2 is used as the join key and the second field is passed
   * as a parameter to the transformation function.
   * @param vertexJoinFunction the transformation function to apply.
   * The first parameter is the current vertex value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @return a new Graph, where the vertex values have been updated according to the
   * result of the vertexJoinFunction.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   */
  def joinWithVertices[T: TypeInformation](inputDataSet: DataSet[(K, T)],
  vertexJoinFunction: VertexJoinFunction[VV, T]): Graph[K, VV, EV] = {
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithVertices[T](javaTupleSet, vertexJoinFunction))
  }

  /**
   * Joins the vertex DataSet of this graph with an input Tuple2 DataSet and applies
   * a user-defined transformation on the values of the matched records.
   * The vertex ID and the first field of the Tuple2 DataSet are used as the join keys.
   * 
   * @param inputDataSet the Tuple2 DataSet to join with.
   * The first field of the Tuple2 is used as the join key and the second field is passed
   * as a parameter to the transformation function.
   * @param fun the transformation function to apply.
   * The first parameter is the current vertex value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @return a new Graph, where the vertex values have been updated according to the
   * result of the vertexJoinFunction.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   */
  def joinWithVertices[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (VV, T) => VV):
  Graph[K, VV, EV] = {
    val newVertexJoin = new VertexJoinFunction[VV, T]() {
      val cleanFun = clean(fun)

      override def vertexJoin(vertexValue: VV, inputValue: T): VV = {
        cleanFun(vertexValue, inputValue)
      }
    }
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithVertices[T](javaTupleSet, newVertexJoin))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the composite key of both
   * source and target IDs and applies a user-defined transformation on the values
   * of the matched records. The first two fields of the input DataSet are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first two fields of the Tuple3 are used as the composite join key
   * and the third field is passed as a parameter to the transformation function.
   * @param edgeJoinFunction the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple3 from the input DataSet.
   * @tparam T the type of the third field of the input Tuple3 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdges[T: TypeInformation](inputDataSet: DataSet[(K, K, T)],
  edgeJoinFunction: EdgeJoinFunction[EV, T]): Graph[K, VV, EV] = {
    val javaTupleSet = inputDataSet.map(
      (i: (K, K, T)) => new jtuple.Tuple3(i._1, i._2, i._3)).javaSet
    wrapGraph(jgraph.joinWithEdges[T](javaTupleSet, edgeJoinFunction))
  }

  /**
   * Joins the edge DataSet with an input DataSet on the composite key of both
   * source and target IDs and applies a user-defined transformation on the values
   * of the matched records. The first two fields of the input DataSet are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first two fields of the Tuple3 are used as the composite join key
   * and the third field is passed as a parameter to the transformation function.
   * @param fun the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple3 from the input DataSet.
   * @tparam T the type of the third field of the input Tuple3 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdges[T: TypeInformation](inputDataSet: DataSet[(K, K, T)], fun: (EV, T) => EV):
  Graph[K, VV, EV] = {
    val newEdgeJoin = new EdgeJoinFunction[EV, T]() {
      val cleanFun = clean(fun)

      override def edgeJoin(edgeValue: EV, inputValue: T): EV = {
        cleanFun(edgeValue, inputValue)
      }
    }
    val javaTupleSet = inputDataSet.map(
      (i: (K, K, T)) => new jtuple.Tuple3(i._1, i._2, i._3)).javaSet
    wrapGraph(jgraph.joinWithEdges[T](javaTupleSet, newEdgeJoin))
  }

  /**
   * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
   * on the values of the matched records.
   * The source ID of the edges input and the first field of the input DataSet
   * are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first field of the Tuple2 is used as the join key
   * and the second field is passed as a parameter to the transformation function.
   * @param edgeJoinFunction the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdgesOnSource[T: TypeInformation](inputDataSet: DataSet[(K, T)],
  edgeJoinFunction: EdgeJoinFunction[EV, T]): Graph[K, VV, EV] = {
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnSource[T](javaTupleSet, edgeJoinFunction))
  }

  /**
   * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
   * on the values of the matched records.
   * The source ID of the edges input and the first field of the input DataSet
   * are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first field of the Tuple2 is used as the join key
   * and the second field is passed as a parameter to the transformation function.
   * @param fun the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdgesOnSource[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (EV, T) =>
    EV): Graph[K, VV, EV] = {
    val newEdgeJoin = new EdgeJoinFunction[EV, T]() {
      val cleanFun = clean(fun)

      override def edgeJoin(edgeValue: EV, inputValue: T): EV = {
        cleanFun(edgeValue, inputValue)
      }
    }
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnSource[T](javaTupleSet, newEdgeJoin))
  }

  /**
   * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
   * on the values of the matched records.
   * The target ID of the edges input and the first field of the input DataSet
   * are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first field of the Tuple2 is used as the join key
   * and the second field is passed as a parameter to the transformation function.
   * @param edgeJoinFunction the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdgesOnTarget[T: TypeInformation](inputDataSet: DataSet[(K, T)],
  edgeJoinFunction: EdgeJoinFunction[EV, T]): Graph[K, VV, EV] = {
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnTarget[T](javaTupleSet, edgeJoinFunction))
  }

  /**
   * Joins the edge DataSet with an input Tuple2 DataSet and applies a user-defined transformation
   * on the values of the matched records.
   * The target ID of the edges input and the first field of the input DataSet
   * are used as join keys.
   * 
   * @param inputDataSet the DataSet to join with.
   * The first field of the Tuple2 is used as the join key
   * and the second field is passed as a parameter to the transformation function.
   * @param fun the transformation function to apply.
   * The first parameter is the current edge value and the second parameter is the value
   * of the matched Tuple2 from the input DataSet.
   * @tparam T the type of the second field of the input Tuple2 DataSet.
   * @return a new Graph, where the edge values have been updated according to the
   * result of the edgeJoinFunction.
   */
  def joinWithEdgesOnTarget[T: TypeInformation](inputDataSet: DataSet[(K, T)], fun: (EV, T) =>
    EV): Graph[K, VV, EV] = {
    val newEdgeJoin = new EdgeJoinFunction[EV, T]() {
      val cleanFun = clean(fun)

      override def edgeJoin(edgeValue: EV, inputValue:T): EV = {
        cleanFun(edgeValue, inputValue)
      }
    }
    val javaTupleSet = inputDataSet.map(
      (i: (K, T)) => new jtuple.Tuple2(i._1, i._2)).javaSet
    wrapGraph(jgraph.joinWithEdgesOnTarget[T](javaTupleSet, newEdgeJoin))
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
  def inDegrees(): DataSet[(K, LongValue)] = {
    wrap(jgraph.inDegrees).map((i: jtuple.Tuple2[K, LongValue]) => (i.f0, i.f1))
  }

  /**
   * Return the out-degree of all vertices in the graph
   *
   * @return A DataSet of Tuple2<vertexId, outDegree>
   */
  def outDegrees(): DataSet[(K, LongValue)] = {
    wrap(jgraph.outDegrees).map((i: jtuple.Tuple2[K, LongValue]) => (i.f0, i.f1))
  }

  /**
   * Return the degree of all vertices in the graph
   *
   * @return A DataSet of Tuple2<vertexId, degree>
   */
  def getDegrees(): DataSet[(K, LongValue)] = {
    wrap(jgraph.getDegrees).map((i: jtuple.Tuple2[K, LongValue]) => (i.f0, i.f1))
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
    wrap(jgraph.getEdgeIds).map((i: jtuple.Tuple2[K, K]) => (i.f0, i.f1))
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
   * @param vertices the list of vertices to add
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
   * @param edges the data set of edges to be added
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
   * @param vertices list of vertices to remove
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
   * @param edges the list of edges to be removed
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
   *
   * @param graph the graph to perform difference with
   * @return a new graph where the common vertices and edges have been removed
   */
  def difference(graph: Graph[K, VV, EV]) = {
    wrapGraph(jgraph.difference(graph.getWrappedGraph))
  }

  /**
   * Performs intersect on the edge sets of the input graphs. Edges are considered equal, if they
   * have the same source identifier, target identifier and edge value.
   * <p>
   * The method computes pairs of equal edges from the input graphs. If the same edge occurs
   * multiple times in the input graphs, there will be multiple edge pairs to be considered. Each
   * edge instance can only be part of one pair. If the given parameter `distinctEdges` is set
   * to `true`, there will be exactly one edge in the output graph representing all pairs of
   * equal edges. If the parameter is set to `false`, both edges of each pair will be in the
   * output.
   * <p>
   * Vertices in the output graph will have no vertex values.
   *
   * @param graph the graph to perform intersect with
   * @param distinctEdges if set to { @code true}, there will be exactly one edge in the output
   *                      graph representing all pairs of equal edges, otherwise, for each pair,
   *                      both edges will be in the output graph
   * @return a new graph which contains only common vertices and edges from the input graphs
   */
  def intersect(graph: Graph[K, VV, EV], distinctEdges: Boolean): Graph[K, NullValue, EV] = {
    wrapGraph(jgraph.intersect(graph.getWrappedGraph, distinctEdges))
  }

  /**
   * Compute a reduce transformation over the neighbors' vertex values of each vertex.
   * For each vertex, the transformation consecutively calls a
   * [[ReduceNeighborsFunction]] until only a single value for each vertex remains.
   * The [[ReduceNeighborsFunction]] combines a pair of neighbor vertex values
   * into one new value of the same type.
   * 
   * @param reduceNeighborsFunction the reduce function to apply to the neighbors of each vertex.
   * @param direction the edge direction (in-, out-, all-)
   * @return a Dataset of Tuple2, with one tuple per vertex.
   * The first field of the Tuple2 is the vertex ID and the second field
   * is the aggregate value computed by the provided [[ReduceNeighborsFunction]].
   */
  def reduceOnNeighbors(reduceNeighborsFunction: ReduceNeighborsFunction[VV], direction:
  EdgeDirection): DataSet[(K, VV)] = {
    wrap(jgraph.reduceOnNeighbors(reduceNeighborsFunction, direction)).map(
      (i: jtuple.Tuple2[K, VV]) => (i.f0, i.f1))
  }

  /**
   * Compute a reduce transformation over the neighbors' vertex values of each vertex.
   * For each vertex, the transformation consecutively calls a
   * [[ReduceNeighborsFunction]] until only a single value for each vertex remains.
   * The [[ReduceNeighborsFunction]] combines a pair of neighbor vertex values
   * into one new value of the same type.
   * 
   * @param reduceEdgesFunction the reduce function to apply to the edges of each vertex.
   * @param direction the edge direction (in-, out-, all-)
   * @return a Dataset of Tuple2, with one tuple per vertex.
   * The first field of the Tuple2 is the vertex ID and the second field
   * is the aggregate value computed by the provided [[ReduceNeighborsFunction]].
   */
  def reduceOnEdges(reduceEdgesFunction: ReduceEdgesFunction[EV], direction: EdgeDirection):
  DataSet[(K, EV)] = {
    wrap(jgraph.reduceOnEdges(reduceEdgesFunction, direction)).map(
      (i: jtuple.Tuple2[K, EV]) => (i.f0, i.f1))
  }

  /**
   * @param algorithm the algorithm to run on the Graph
   * @return the result of the graph algorithm
   */
  def run[T: TypeInformation : ClassTag](algorithm: GraphAlgorithm[K, VV, EV, T]):
  T = {
    jgraph.run(algorithm)
  }

  /**
   * A GraphAnalytic is similar to a GraphAlgorithm but is terminal and results
   * are retrieved via accumulators.  A Flink program has a single point of
   * execution. A GraphAnalytic defers execution to the user to allow composing
   * multiple analytics and algorithms into a single program.
   *
   * @param analytic the analytic to run on the Graph
   */
  def run[T: TypeInformation : ClassTag](analytic: GraphAnalytic[K, VV, EV, T]):
  GraphAnalytic[K, VV, EV, T] = {
    jgraph.run(analytic)
    analytic
  }

  /**
   * Runs a scatter-gather iteration on the graph.
   * No configuration options are provided.
   *
   * @param scatterFunction the scatter function
   * @param gatherFunction the gather function
   * @param maxIterations maximum number of iterations to perform
   * @return the updated Graph after the scatter-gather iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runScatterGatherIteration[M](scatterFunction: ScatterFunction[K, VV, M, EV],
                                   gatherFunction: SpargelGatherFunction[K, VV, M],
                                   maxIterations: Int): Graph[K, VV, EV] = {
    wrapGraph(jgraph.runScatterGatherIteration(scatterFunction, gatherFunction,
      maxIterations))
  }

  /**
   * Runs a scatter-gather iteration on the graph with configuration options.
   *
   * @param scatterFunction the scatter function
   * @param gatherFunction the gather function
   * @param maxIterations maximum number of iterations to perform
   * @param parameters the iteration configuration parameters
   * @return the updated Graph after the scatter-gather iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runScatterGatherIteration[M](scatterFunction: ScatterFunction[K, VV, M, EV],
                                   gatherFunction: SpargelGatherFunction[K, VV, M],
                                   maxIterations: Int, parameters: ScatterGatherConfiguration):
  Graph[K, VV, EV] = {
    wrapGraph(jgraph.runScatterGatherIteration(scatterFunction, gatherFunction,
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
   * @return the updated Graph after the gather-sum-apply iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runGatherSumApplyIteration[M](gatherFunction: GSAGatherFunction[VV, EV, M], sumFunction:
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
   * @return the updated Graph after the gather-sum-apply iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runGatherSumApplyIteration[M](gatherFunction: GSAGatherFunction[VV, EV, M], sumFunction:
  SumFunction[VV, EV, M], applyFunction: ApplyFunction[K, VV, M], maxIterations: Int,
                                    parameters: GSAConfiguration): Graph[K, VV, EV] = {
    wrapGraph(jgraph.runGatherSumApplyIteration(gatherFunction, sumFunction, applyFunction,
      maxIterations, parameters))
  }

  /**
   * Runs a vertex-centric iteration on the graph.
   * No configuration options are provided.
   *
   * @param computeFunction the compute function
   * @param combineFunction the optional message combiner function
   * @param maxIterations maximum number of iterations to perform
   * @return the updated Graph after the vertex-centric iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runVertexCentricIteration[M](computeFunction: ComputeFunction[K, VV, EV, M],
                                   combineFunction: MessageCombiner[K, M],
                                   maxIterations: Int): Graph[K, VV, EV] = {
    wrapGraph(jgraph.runVertexCentricIteration(computeFunction, combineFunction,
      maxIterations))
  }

  /**
   * Runs a vertex-centric iteration on the graph with configuration options.
   *
   * @param computeFunction the compute function
   * @param combineFunction the optional message combiner function
   * @param maxIterations maximum number of iterations to perform
   * @param parameters the iteration configuration parameters
   * @return the updated Graph after the vertex-centric iteration has converged or
   *         after maximumNumberOfIterations.
   */
  def runVertexCentricIteration[M](computeFunction: ComputeFunction[K, VV, EV, M],
                                   combineFunction: MessageCombiner[K, M],
                                   maxIterations: Int, parameters: VertexCentricConfiguration):
  Graph[K, VV, EV] = {
    wrapGraph(jgraph.runVertexCentricIteration(computeFunction, combineFunction,
      maxIterations, parameters))
  }

  def validate(validator: GraphValidator[K, VV, EV]): Boolean = {
    jgraph.validate(validator)
  }

}
