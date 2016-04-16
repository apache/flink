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

package org.apache.flink.graph.translate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

/**
 * Methods for translation of the type or modification of the data of graph
 * labels, vertex values, and edge values.
 */
public class Translate {

	// --------------------------------------------------------------------------------------------
	//  Translate graph labels
	// --------------------------------------------------------------------------------------------

	/**
	 * Relabels {@link Vertex Vertices} and {@link Edge}s of a {@link Graph} using the given {@link Translator}.
	 *
	 * @param graph input graph
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <OLD> old graph label type
	 * @param <NEW> new graph label type
	 * @param <VV> vertex value type
	 * @param <EV> edge value type
	 * @return translated graph
	 */
	public static <OLD,NEW,VV,EV> Graph<NEW,VV,EV> translateGraphLabels(Graph<OLD,VV,EV> graph, Translator<OLD,NEW> translator) {
		return translateGraphLabels(graph, translator, ExecutionConfig.PARALLELISM_UNKNOWN);
	}

	/**
	 * Relabels {@link Vertex Vertices} and {@link Edge}s of a {@link Graph} using the given {@link Translator}.
	 *
	 * @param graph input graph
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <OLD> old graph label type
	 * @param <NEW> new graph label type
	 * @param <VV> vertex value type
	 * @param <EV> edge value type
	 * @return translated graph
	 */
	public static <OLD,NEW,VV,EV> Graph<NEW,VV,EV> translateGraphLabels(Graph<OLD,VV,EV> graph, Translator<OLD,NEW> translator, int parallelism) {
		// Vertices
		DataSet<Vertex<NEW,VV>> translatedVertices = translateVertexLabels(graph.getVertices(), translator, parallelism);

		// Edges
		DataSet<Edge<NEW,EV>> translatedEdges = translateEdgeLabels(graph.getEdges(), translator, parallelism);

		// Graph
		return Graph.fromDataSet(translatedVertices, translatedEdges, graph.getContext());
	}

	// --------------------------------------------------------------------------------------------
	//  Translate vertex labels
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Vertex} labels using the given {@link Translator}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <OLD> old vertex label type
	 * @param <NEW> new vertex label type
	 * @param <VV> vertex value type
	 * @return translated vertices
	 */
	public static <OLD,NEW,VV> DataSet<Vertex<NEW,VV>> translateVertexLabels(DataSet<Vertex<OLD,VV>> vertices, Translator<OLD,NEW> translator) {
		return translateVertexLabels(vertices, translator, ExecutionConfig.PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Vertex} labels using the given {@link Translator}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <OLD> old vertex label type
	 * @param <NEW> new vertex label type
	 * @param <VV> vertex value type
	 * @return translated vertices
	 */
	@SuppressWarnings("unchecked")
	public static <OLD,NEW,VV> DataSet<Vertex<NEW,VV>> translateVertexLabels(DataSet<Vertex<OLD,VV>> vertices, Translator<OLD,NEW> translator, int parallelism) {
		Class<Vertex<NEW,VV>> vertexClass = (Class<Vertex<NEW,VV>>)(Class<? extends Vertex>) Vertex.class;
		TypeInformation<NEW> newType = translator.getTypeHint().getTypeInfo();
		TypeInformation<VV> vertexValueType = ((TupleTypeInfo<Vertex<OLD,VV>>) vertices.getType()).getTypeAt(1);

		TupleTypeInfo<Vertex<NEW,VV>> returnType = new TupleTypeInfo<>(vertexClass, newType, vertexValueType);

		return vertices
			.map(new TranslateVertexLabel<OLD,NEW,VV>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate vertex labels");
	}

	/**
	 * Translate {@link Vertex} labels using the given {@link Translator}.
	 *
	 * @param <OLD> old vertex label type
	 * @param <NEW> new vertex label type
	 * @param <VV> vertex value type
	 */
	@ForwardedFields("1")
	private static class TranslateVertexLabel<OLD,NEW,VV>
	implements MapFunction<Vertex<OLD,VV>, Vertex<NEW,VV>> {
		private final Translator<OLD,NEW> translator;

		private Vertex<NEW,VV> vertex = new Vertex<>();

		public TranslateVertexLabel(Translator<OLD,NEW> translator) {
			this.translator = translator;
		}

		@Override
		public Vertex<NEW,VV> map(Vertex<OLD,VV> value)
				throws Exception {
			vertex.f0 = translator.translate(value.f0, vertex.f0);
			vertex.f1 = value.f1;

			return vertex;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate edge labels
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Edge} labels using the given {@link Translator}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <OLD> old edge label type
	 * @param <NEW> new edge label type
	 * @param <EV> edge value type
	 * @return translated edges
	 */
	public static <OLD,NEW,EV> DataSet<Edge<NEW,EV>> translateEdgeLabels(DataSet<Edge<OLD,EV>> edges, Translator<OLD,NEW> translator) {
		return translateEdgeLabels(edges, translator, ExecutionConfig.PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Edge} labels using the given {@link Translator}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <OLD> old edge label type
	 * @param <NEW> new edge label type
	 * @param <EV> edge value type
	 * @return translated edges
	 */
	@SuppressWarnings("unchecked")
	public static <OLD,NEW,EV> DataSet<Edge<NEW,EV>> translateEdgeLabels(DataSet<Edge<OLD,EV>> edges, Translator<OLD,NEW> translator, int parallelism) {
		Class<Edge<NEW,EV>> edgeClass = (Class<Edge<NEW,EV>>)(Class<? extends Edge>) Edge.class;
		TypeInformation<NEW> newType = translator.getTypeHint().getTypeInfo();
		TypeInformation<EV> edgeValueType = ((TupleTypeInfo<Edge<OLD,EV>>) edges.getType()).getTypeAt(2);

		TupleTypeInfo<Edge<NEW,EV>> returnType = new TupleTypeInfo<>(edgeClass, newType, newType, edgeValueType);

		return edges
			.map(new TranslateEdgeLabel<OLD,NEW,EV>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate edge labels");
	}

	/**
	 * Translate {@link Edge} labels using the given {@link Translator}.
	 *
	 * @param <OLD> old edge label type
	 * @param <NEW> new edge label type
	 * @param <EV> edge label type
	 */
	@ForwardedFields("2")
	private static class TranslateEdgeLabel<OLD,NEW,EV>
	implements MapFunction<Edge<OLD,EV>, Edge<NEW,EV>> {
		private final Translator<OLD,NEW> translator;

		private Edge<NEW,EV> edge = new Edge<>();

		public TranslateEdgeLabel(Translator<OLD,NEW> translator) {
			this.translator = translator;
		}

		@Override
		public Edge<NEW,EV> map(Edge<OLD,EV> value)
				throws Exception {
			edge.f0 = translator.translate(value.f0, edge.f0);
			edge.f1 = translator.translate(value.f1, edge.f1);
			edge.f2 = value.f2;

			return edge;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate vertex values
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Vertex} values using the given {@link Translator}.
	 *
	 * @param vertices input vertices
	 * @param <K> vertex label type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 * @return translated vertices
	 */
	public static <K,OLD,NEW> DataSet<Vertex<K,NEW>> translateVertexValues(DataSet<Vertex<K,OLD>> vertices, Translator<OLD,NEW> translator) {
		return translateVertexValues(vertices, translator, ExecutionConfig.PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Vertex} values using the given {@link Translator}.
	 *
	 * @param vertices source vertices
	 * @param parallelism operator parallelism
	 * @param <K> vertex label type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 * @return translated vertices
	 */
	@SuppressWarnings("unchecked")
	public static <K,OLD,NEW> DataSet<Vertex<K,NEW>> translateVertexValues(DataSet<Vertex<K,OLD>> vertices, Translator<OLD,NEW> translator, int parallelism) {
		Class<Vertex<K,NEW>> vertexClass = (Class<Vertex<K,NEW>>)(Class<? extends Vertex>) Vertex.class;
		TypeInformation<K> labelType = ((TupleTypeInfo<Vertex<K,OLD>>) vertices.getType()).getTypeAt(0);
		TypeInformation<NEW> newType = translator.getTypeHint().getTypeInfo();

		TupleTypeInfo<Vertex<K,NEW>> returnType = new TupleTypeInfo<>(vertexClass, labelType, newType);

		return vertices
			.map(new TranslateVertexValue<K,OLD,NEW>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate vertex values");
	}

	/**
	 * Translate {@link Vertex} values using the given {@link Translator}.
	 *
	 * @param <K> vertex label type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 */
	@ForwardedFields("0")
	private static class TranslateVertexValue<K,OLD,NEW>
	implements MapFunction<Vertex<K,OLD>, Vertex<K,NEW>> {
		private final Translator<OLD,NEW> translator;

		private Vertex<K,NEW> vertex = new Vertex<>();

		public TranslateVertexValue(Translator<OLD,NEW> translator) {
			this.translator = translator;
		}

		@Override
		public Vertex<K,NEW> map(Vertex<K,OLD> value)
				throws Exception {
			vertex.f0 = value.f0;
			vertex.f1 = translator.translate(value.f1, vertex.f1);

			return vertex;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate edge values
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Edge} values using the given {@link Translator}.
	 *
	 * @param edges source edges
	 * @param <K> edge label type
	 * @param <OLD> old edge value type
	 * @param <NEW> new edge value type
	 * @return relabeled edges
	 */
	public static <K,OLD,NEW> DataSet<Edge<K,NEW>> translateEdgeValues(DataSet<Edge<K,OLD>> edges, Translator<OLD,NEW> translator) {
		return translateEdgeValues(edges, translator, ExecutionConfig.PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Edge} values using the given {@link Translator}.
	 *
	 * @param edges source edges
	 * @param parallelism operator parallelism
	 * @param <K> vertex label type
	 * @return relabeled edges
	 */
	@SuppressWarnings("unchecked")
	public static <K,OLD,NEW> DataSet<Edge<K,NEW>> translateEdgeValues(DataSet<Edge<K,OLD>> edges, Translator<OLD,NEW> translator, int parallelism) {
		Class<Edge<K,NEW>> edgeClass = (Class<Edge<K,NEW>>)(Class<? extends Edge>) Edge.class;
		TypeInformation<K> labelType = ((TupleTypeInfo<Edge<K,OLD>>) edges.getType()).getTypeAt(0);
		TypeInformation<NEW> newType = translator.getTypeHint().getTypeInfo();

		TupleTypeInfo<Edge<K,NEW>> returnType = new TupleTypeInfo<>(edgeClass, labelType, labelType, newType);

		return edges
			.map(new TranslateEdgeValue<K,OLD,NEW>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Relabel edge values");
	}

	/**
	 * Translate {@link Edge} values using the given {@link Translator}.
	 *
	 * @param <K> edge label type
	 * @param <OLD> old edge value type
	 * @param <NEW> new edge value type
	 */
	@ForwardedFields("0; 1")
	private static class TranslateEdgeValue<K,OLD,NEW>
	implements MapFunction<Edge<K,OLD>, Edge<K,NEW>> {
		private final Translator<OLD,NEW> translator;

		private Edge<K,NEW> edge = new Edge<>();

		public TranslateEdgeValue(Translator<OLD,NEW> translator) {
			this.translator = translator;
		}

		@Override
		public Edge<K,NEW> map(Edge<K,OLD> value)
				throws Exception {
			edge.f0 = value.f0;
			edge.f1 = value.f1;
			edge.f2 = translator.translate(value.f2, edge.f2);

			return edge;
		}
	}
}
