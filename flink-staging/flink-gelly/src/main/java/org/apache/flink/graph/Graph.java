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

package flink.graphs;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collection;


@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> implements Serializable{

	private final DataSet<Tuple2<K, VV>> vertices;

	private final DataSet<Tuple3<K, K, EV>> edges;

	private boolean isUndirected;

	private static TypeInformation<?> keyType;
	private static TypeInformation<?> vertexValueType;
	private static TypeInformation<?> edgeValueType;


	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges) {

		/** a graph is directed by default */
		this(vertices, edges, false);
	}

	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
		this.isUndirected = undirected;
		
		Graph.keyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
		Graph.vertexValueType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(1);
		Graph.edgeValueType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(2);
	}

	public DataSet<Tuple2<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Tuple3<K, K, EV>> getEdges() {
		return edges;
	}
    
    /**
     * Apply a function to the attribute of each vertex in the graph.
     * @param mapper
     * @return
     */
    public <NV extends Serializable> DataSet<Tuple2<K, NV>> mapVertices(final MapFunction<VV, NV> mapper) {
        return vertices.map(new ApplyMapperToVertexWithType<K, VV, NV>(mapper));
    }
    
    private static final class ApplyMapperToVertexWithType<K, VV, NV> implements MapFunction
		<Tuple2<K, VV>, Tuple2<K, NV>>, ResultTypeQueryable<Tuple2<K, NV>> {
	
		private MapFunction<VV, NV> innerMapper;
		
		public ApplyMapperToVertexWithType(MapFunction<VV, NV> theMapper) {
			this.innerMapper = theMapper;
		}
		
		public Tuple2<K, NV> map(Tuple2<K, VV> value) throws Exception {
			return new Tuple2<K, NV>(value.f0, innerMapper.map(value.f1));
		}
	
		@Override
		public TypeInformation<Tuple2<K, NV>> getProducedType() {
			@SuppressWarnings("unchecked")
			TypeInformation<NV> newVertexValueType = TypeExtractor.getMapReturnTypes(innerMapper, 
					(TypeInformation<VV>)vertexValueType);
			
			return new TupleTypeInfo<Tuple2<K, NV>>(keyType, newVertexValueType);
		}
    }
    
    /**
     * Apply a function to the attribute of each edge in the graph.
     * @param mapper
     * @return 
     */
    public <NV extends Serializable> DataSet<Tuple3<K, K, NV>> mapEdges(final MapFunction<EV, NV> mapper) {
        return edges.map(new ApplyMapperToEdgeWithType<K, EV, NV>(mapper));
    }
    
    private static final class ApplyMapperToEdgeWithType<K, EV, NV> implements MapFunction
		<Tuple3<K, K, EV>, Tuple3<K, K, NV>>, ResultTypeQueryable<Tuple3<K, K, NV>> {
	
		private MapFunction<EV, NV> innerMapper;
		
		public ApplyMapperToEdgeWithType(MapFunction<EV, NV> theMapper) {
			this.innerMapper = theMapper;
		}
		
		public Tuple3<K, K, NV> map(Tuple3<K, K, EV> value) throws Exception {
			return new Tuple3<K, K, NV>(value.f0, value.f1, innerMapper.map(value.f2));
		}
	
		@Override
		public TypeInformation<Tuple3<K, K, NV>> getProducedType() {
			@SuppressWarnings("unchecked")
			TypeInformation<NV> newEdgeValueType = TypeExtractor.getMapReturnTypes(innerMapper, 
					(TypeInformation<EV>)edgeValueType);
			
			return new TupleTypeInfo<Tuple3<K, K, NV>>(keyType, keyType, newEdgeValueType);
		}
    }

    /**
     * Apply value-based filtering functions to the graph 
     * and return a sub-graph that satisfies the predicates
     * for both vertex values and edge values.
     * @param vertexFilter
     * @param edgeFilter
     * @return
     */
    public Graph<K, VV, EV> subgraph(FilterFunction<VV> vertexFilter, FilterFunction<EV> edgeFilter) {

        DataSet<Tuple2<K, VV>> filteredVertices = this.vertices.filter(
        		new ApplyVertexFilter<K, VV>(vertexFilter));

        DataSet<Tuple3<K, K, EV>> remainingEdges = this.edges.join(filteredVertices)
        		.where(0).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>())
        		.join(filteredVertices).where(1).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>());

        DataSet<Tuple3<K, K, EV>> filteredEdges = remainingEdges.filter(
        		new ApplyEdgeFilter<K, EV>(edgeFilter));

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges);
    }
    
    @ConstantFieldsFirst("0->0;1->1;2->2")
    private static final class ProjectEdge<K, VV, EV> implements FlatJoinFunction<Tuple3<K,K,EV>, Tuple2<K,VV>, 
		Tuple3<K,K,EV>> {
		public void join(Tuple3<K, K, EV> first,
				Tuple2<K, VV> second, Collector<Tuple3<K, K, EV>> out) {
			out.collect(first);
		}
    }
    
    private static final class ApplyVertexFilter<K, VV> implements FilterFunction<Tuple2<K, VV>> {

    	private FilterFunction<VV> innerFilter;
    	
    	public ApplyVertexFilter(FilterFunction<VV> theFilter) {
    		this.innerFilter = theFilter;
    	}

		public boolean filter(Tuple2<K, VV> value) throws Exception {
			return innerFilter.filter(value.f1);
		}
    	
    }

    private static final class ApplyEdgeFilter<K, EV> implements FilterFunction<Tuple3<K, K, EV>> {

    	private FilterFunction<EV> innerFilter;
    	
    	public ApplyEdgeFilter(FilterFunction<EV> theFilter) {
    		this.innerFilter = theFilter;
    	}    	
        public boolean filter(Tuple3<K, K, EV> value) throws Exception {
            return innerFilter.filter(value.f2);
        }
    }

    /**
     * Return the out-degree of all vertices in the graph
     * @return A DataSet of Tuple2<vertexId, outDegree>
     */
    public DataSet<Tuple2<K, Long>> outDegrees() {

    	return vertices.join(edges).where(0).equalTo(0).map(new VertexKeyWithOne<K, EV, VV>())
    			.groupBy(0).sum(1);
    	}

    private static final class VertexKeyWithOne<K, EV, VV> implements
    	MapFunction<Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>>, Tuple2<K, Long>> {

		public Tuple2<K, Long> map(
				Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>> value) {
			return new Tuple2<K, Long>(value.f0.f0, 1L);
		}
    }
		
    /**
     * Push-Gather-Apply model of graph computation
     * @param cog
     * @param gred
     * @param fjoin
     * @param maxIterations
     * @param <MsgT>
     * @return
     */
    public <MsgT> Graph<K, VV, EV> pga(CoGroupFunction<Tuple2<K, VV>, Tuple3<K, K, EV>, Tuple2<K, MsgT>> cog,
                                       GroupReduceFunction<Tuple2<K, MsgT>, Tuple2<K, MsgT>> gred,
                                       FlatJoinFunction<Tuple2<K, MsgT>, Tuple2<K, VV>, Tuple2<K, VV>> fjoin,
                                       int maxIterations){

        DeltaIteration<Tuple2<K, VV>, Tuple2<K, VV>> iteration = this.vertices
            .iterateDelta(this.vertices, maxIterations, 0);

        DataSet<Tuple2<K, MsgT>> p = iteration.getWorkset().coGroup(this.edges).where(0).equalTo(0).with(cog);

        DataSet<Tuple2<K, MsgT>> g = p.groupBy(0).reduceGroup(gred);

        DataSet<Tuple2<K, VV>> a = g.join(iteration.getSolutionSet()).where(0).equalTo(0).with(fjoin);

        DataSet<Tuple2<K, VV>> result = iteration.closeWith(a, a);

        return new Graph<>(result, this.edges);
    }

	/**
	 * Convert the directed graph into an undirected graph
	 * by adding all inverse-direction edges.
	 *
	 */
	public Graph<K, VV, EV> getUndirected() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Tuple3<K, K, EV>> undirectedEdges =
					edges.union(edges.map(new ReverseEdgesMap<K, EV>()));
			return new Graph<K, VV, EV>(vertices, undirectedEdges, true);
			}
	}

	@ConstantFields("0->1;1->0;2->2")
	private static final class ReverseEdgesMap<K, EV> implements MapFunction<Tuple3<K, K, EV>,
		Tuple3<K, K, EV>> {

		public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) {
			return new Tuple3<K, K, EV>(value.f1, value.f0, value.f2);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Tuple3<K, K, EV>> undirectedEdges = edges.map(new ReverseEdgesMap<K, EV>());
			return new Graph<K, VV, EV>(vertices, (DataSet<Tuple3<K, K, EV>>) undirectedEdges, true);
		}
	}

	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV>
		create(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges) {
		return new Graph<K, VV, EV>(vertices, edges);
	}

	/**
	 * Read and create the graph Tuple2 dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param Tuple2IdClass
	 * @param Tuple2ValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable>
		DataSet<Tuple2<K, VV>> readTuple2CsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Tuple2<K, VV>> vertices = reader.fieldDelimiter(delimiter).types(Tuple2IdClass, Tuple2ValueClass)
		.map(new MapFunction<Tuple2<K, VV>, Tuple2<K, VV>>() {

			public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Tuple2<K, VV>)value;
			}
		});
		return vertices;
	}

	/**
	 * Read and create the graph edge dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param Tuple2IdClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, EV extends Serializable>
		DataSet<Tuple3<K, K, EV>> readEdgesCsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> Tuple2IdClass, Class<EV> edgeValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Tuple3<K, K, EV>> edges = reader.fieldDelimiter(delimiter)
			.types(Tuple2IdClass, Tuple2IdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {

			public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Tuple3<K, K, EV>)value;
			}
		});
		return edges;
	}

	/**
	 * Create the graph, by reading a csv file for vertices
	 * and a csv file for the edges
	 * @param env
	 * @param Tuple2Filepath
	 * @param Tuple2Delimiter
	 * @param edgeFilepath
	 * @param edgeDelimiter
	 * @param Tuple2IdClass
	 * @param Tuple2ValueClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV> readGraphFromCsvFile(ExecutionEnvironment env,
				String Tuple2Filepath, char Tuple2Delimiter, String edgeFilepath, char edgeDelimiter,
				Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass,	Class<EV> edgeValueClass) {

		CsvReader Tuple2Reader = new CsvReader(Tuple2Filepath, env);
		DataSet<Tuple2<K, VV>> vertices = Tuple2Reader.fieldDelimiter(Tuple2Delimiter)
				.types(Tuple2IdClass, Tuple2ValueClass).map(new MapFunction<Tuple2<K, VV>,
						Tuple2<K, VV>>() {

			public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Tuple2<K, VV>)value;
			}
		});

		CsvReader edgeReader = new CsvReader(edgeFilepath, env);
		DataSet<Tuple3<K, K, EV>> edges = edgeReader.fieldDelimiter(edgeDelimiter)
			.types(Tuple2IdClass, Tuple2IdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {

			public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Tuple3<K, K, EV>)value;
			}
		});

		return Graph.create(vertices, edges);
	}

	/**
     	 * Creates a graph from the given vertex and edge collections
     	 * @param env
     	 * @param v the collection of vertices
         * @param e the collection of edges
         * @return a new graph formed from the set of edges and vertices
         */
	 public static <K extends Comparable<K> & Serializable, VV extends Serializable,
			EV extends Serializable> Graph<K, VV, EV> fromCollection(ExecutionEnvironment env, Collection<Tuple2<K, VV>> v,
                                           Collection<Tuple3<K, K, EV>> e) throws Exception {
		DataSet<Tuple2<K, VV>> vertices = env.fromCollection(v);
		DataSet<Tuple3<K, K, EV>> edges = env.fromCollection(e);

		return Graph.create(vertices, edges);
	}

}
