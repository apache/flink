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

package org.apache.flink.graph.asm.translate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;
import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_UNKNOWN;

/**
 * Methods for translation of the type or modification of the data of graph
 * IDs, vertex values, and edge values.
 */
public class Translate {

	// --------------------------------------------------------------------------------------------
	//  Translate vertex IDs
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Vertex} IDs using the given {@link MapFunction}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <OLD> old vertex ID type
	 * @param <NEW> new vertex ID type
	 * @param <VV> vertex value type
	 * @return translated vertices
	 */
	public static <OLD, NEW, VV> DataSet<Vertex<NEW, VV>> translateVertexIds(DataSet<Vertex<OLD, VV>> vertices, MapFunction<OLD, NEW> translator) {
		return translateVertexIds(vertices, translator, PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Vertex} IDs using the given {@link MapFunction}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <OLD> old vertex ID type
	 * @param <NEW> new vertex ID type
	 * @param <VV> vertex value type
	 * @return translated vertices
	 */
	@SuppressWarnings("unchecked")
	public static <OLD, NEW, VV> DataSet<Vertex<NEW, VV>> translateVertexIds(DataSet<Vertex<OLD, VV>> vertices, MapFunction<OLD, NEW> translator, int parallelism) {
		Preconditions.checkNotNull(vertices);
		Preconditions.checkNotNull(translator);
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		Class<Vertex<NEW, VV>> vertexClass = (Class<Vertex<NEW, VV>>)(Class<? extends Vertex>) Vertex.class;
		TypeInformation<NEW> newType = TypeExtractor.createTypeInfo(MapFunction.class, translator.getClass(), 1, null, null);
		TypeInformation<VV> vertexValueType = ((TupleTypeInfo<Vertex<OLD, VV>>) vertices.getType()).getTypeAt(1);

		TupleTypeInfo<Vertex<NEW, VV>> returnType = new TupleTypeInfo<>(vertexClass, newType, vertexValueType);

		return vertices
			.map(new TranslateVertexId<OLD, NEW, VV>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate vertex IDs");
	}

	/**
	 * Translate {@link Vertex} IDs using the given {@link MapFunction}.
	 *
	 * @param <OLD> old vertex ID type
	 * @param <NEW> new vertex ID type
	 * @param <VV> vertex value type
	 */
	@ForwardedFields("1")
	private static class TranslateVertexId<OLD, NEW, VV>
	extends WrappingFunction<MapFunction<OLD, NEW>>
	implements MapFunction<Vertex<OLD, VV>, Vertex<NEW, VV>> {
		private Vertex<NEW, VV> vertex = new Vertex<>();

		public TranslateVertexId(MapFunction<OLD, NEW> translator) {
			super(translator);
		}

		@Override
		public Vertex<NEW, VV> map(Vertex<OLD, VV> value)
				throws Exception {
			vertex.f0 = wrappedFunction.map(value.f0);
			vertex.f1 = value.f1;

			return vertex;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate edge IDs
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Edge} IDs using the given {@link MapFunction}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <OLD> old edge ID type
	 * @param <NEW> new edge ID type
	 * @param <EV> edge value type
	 * @return translated edges
	 */
	public static <OLD, NEW, EV> DataSet<Edge<NEW, EV>> translateEdgeIds(DataSet<Edge<OLD, EV>> edges, MapFunction<OLD, NEW> translator) {
		return translateEdgeIds(edges, translator, PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Edge} IDs using the given {@link MapFunction}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <OLD> old edge ID type
	 * @param <NEW> new edge ID type
	 * @param <EV> edge value type
	 * @return translated edges
	 */
	@SuppressWarnings("unchecked")
	public static <OLD, NEW, EV> DataSet<Edge<NEW, EV>> translateEdgeIds(DataSet<Edge<OLD, EV>> edges, MapFunction<OLD, NEW> translator, int parallelism) {
		Preconditions.checkNotNull(edges);
		Preconditions.checkNotNull(translator);
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		Class<Edge<NEW, EV>> edgeClass = (Class<Edge<NEW, EV>>)(Class<? extends Edge>) Edge.class;
		TypeInformation<NEW> newType = TypeExtractor.createTypeInfo(MapFunction.class, translator.getClass(), 1, null, null);
		TypeInformation<EV> edgeValueType = ((TupleTypeInfo<Edge<OLD, EV>>) edges.getType()).getTypeAt(2);

		TupleTypeInfo<Edge<NEW, EV>> returnType = new TupleTypeInfo<>(edgeClass, newType, newType, edgeValueType);

		return edges
			.map(new TranslateEdgeId<OLD, NEW, EV>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate edge IDs");
	}

	/**
	 * Translate {@link Edge} IDs using the given {@link MapFunction}.
	 *
	 * @param <OLD> old edge ID type
	 * @param <NEW> new edge ID type
	 * @param <EV> edge value type
	 */
	@ForwardedFields("2")
	private static class TranslateEdgeId<OLD, NEW, EV>
	extends WrappingFunction<MapFunction<OLD, NEW>>
	implements MapFunction<Edge<OLD, EV>, Edge<NEW, EV>> {
		private Edge<NEW, EV> edge = new Edge<>();

		public TranslateEdgeId(MapFunction<OLD, NEW> translator) {
			super(translator);
		}

		@Override
		public Edge<NEW, EV> map(Edge<OLD, EV> value)
				throws Exception {
			edge.f0 = wrappedFunction.map(value.f0);
			edge.f1 = wrappedFunction.map(value.f1);
			edge.f2 = value.f2;

			return edge;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate vertex values
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Vertex} values using the given {@link MapFunction}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <K> vertex ID type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 * @return translated vertices
	 */
	public static <K, OLD, NEW> DataSet<Vertex<K, NEW>> translateVertexValues(DataSet<Vertex<K, OLD>> vertices, MapFunction<OLD, NEW> translator) {
		return translateVertexValues(vertices, translator, PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Vertex} values using the given {@link MapFunction}.
	 *
	 * @param vertices input vertices
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <K> vertex ID type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 * @return translated vertices
	 */
	@SuppressWarnings("unchecked")
	public static <K, OLD, NEW> DataSet<Vertex<K, NEW>> translateVertexValues(DataSet<Vertex<K, OLD>> vertices, MapFunction<OLD, NEW> translator, int parallelism) {
		Preconditions.checkNotNull(vertices);
		Preconditions.checkNotNull(translator);
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		Class<Vertex<K, NEW>> vertexClass = (Class<Vertex<K, NEW>>)(Class<? extends Vertex>) Vertex.class;
		TypeInformation<K> idType = ((TupleTypeInfo<Vertex<K, OLD>>) vertices.getType()).getTypeAt(0);
		TypeInformation<NEW> newType = TypeExtractor.createTypeInfo(MapFunction.class, translator.getClass(), 1, null, null);

		TupleTypeInfo<Vertex<K, NEW>> returnType = new TupleTypeInfo<>(vertexClass, idType, newType);

		return vertices
			.map(new TranslateVertexValue<K, OLD, NEW>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate vertex values");
	}

	/**
	 * Translate {@link Vertex} values using the given {@link MapFunction}.
	 *
	 * @param <K> vertex ID type
	 * @param <OLD> old vertex value type
	 * @param <NEW> new vertex value type
	 */
	@ForwardedFields("0")
	private static class TranslateVertexValue<K, OLD, NEW>
	extends WrappingFunction<MapFunction<OLD, NEW>>
	implements MapFunction<Vertex<K, OLD>, Vertex<K, NEW>> {
		private Vertex<K, NEW> vertex = new Vertex<>();

		public TranslateVertexValue(MapFunction<OLD, NEW> translator) {
			super(translator);
		}

		@Override
		public Vertex<K, NEW> map(Vertex<K, OLD> value)
				throws Exception {
			vertex.f0 = value.f0;
			vertex.f1 = wrappedFunction.map(value.f1);

			return vertex;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Translate edge values
	// --------------------------------------------------------------------------------------------

	/**
	 * Translate {@link Edge} values using the given {@link MapFunction}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param <K> edge ID type
	 * @param <OLD> old edge value type
	 * @param <NEW> new edge value type
	 * @return translated edges
	 */
	public static <K, OLD, NEW> DataSet<Edge<K, NEW>> translateEdgeValues(DataSet<Edge<K, OLD>> edges, MapFunction<OLD, NEW> translator) {
		return translateEdgeValues(edges, translator, PARALLELISM_UNKNOWN);
	}

	/**
	 * Translate {@link Edge} values using the given {@link MapFunction}.
	 *
	 * @param edges input edges
	 * @param translator implements conversion from {@code OLD} to {@code NEW}
	 * @param parallelism operator parallelism
	 * @param <K> vertex ID type
	 * @param <OLD> old edge value type
	 * @param <NEW> new edge value type
	 * @return translated edges
	 */
	@SuppressWarnings("unchecked")
	public static <K, OLD, NEW> DataSet<Edge<K, NEW>> translateEdgeValues(DataSet<Edge<K, OLD>> edges, MapFunction<OLD, NEW> translator, int parallelism) {
		Preconditions.checkNotNull(edges);
		Preconditions.checkNotNull(translator);
		Preconditions.checkArgument(parallelism > 0 || parallelism == PARALLELISM_DEFAULT || parallelism == PARALLELISM_UNKNOWN,
			"The parallelism must be greater than zero.");

		Class<Edge<K, NEW>> edgeClass = (Class<Edge<K, NEW>>)(Class<? extends Edge>) Edge.class;
		TypeInformation<K> idType = ((TupleTypeInfo<Edge<K, OLD>>) edges.getType()).getTypeAt(0);
		TypeInformation<NEW> newType = TypeExtractor.createTypeInfo(MapFunction.class, translator.getClass(), 1, null, null);

		TupleTypeInfo<Edge<K, NEW>> returnType = new TupleTypeInfo<>(edgeClass, idType, idType, newType);

		return edges
			.map(new TranslateEdgeValue<K, OLD, NEW>(translator))
			.returns(returnType)
				.setParallelism(parallelism)
				.name("Translate edge values");
	}

	/**
	 * Translate {@link Edge} values using the given {@link MapFunction}.
	 *
	 * @param <K> edge ID type
	 * @param <OLD> old edge value type
	 * @param <NEW> new edge value type
	 */
	@ForwardedFields("0; 1")
	private static class TranslateEdgeValue<K, OLD, NEW>
	extends WrappingFunction<MapFunction<OLD, NEW>>
	implements MapFunction<Edge<K, OLD>, Edge<K, NEW>> {
		private Edge<K, NEW> edge = new Edge<>();

		public TranslateEdgeValue(MapFunction<OLD, NEW> translator) {
			super(translator);
		}

		@Override
		public Edge<K, NEW> map(Edge<K, OLD> value)
				throws Exception {
			edge.f0 = value.f0;
			edge.f1 = value.f1;
			edge.f2 = wrappedFunction.map(value.f2);

			return edge;
		}
	}
}
