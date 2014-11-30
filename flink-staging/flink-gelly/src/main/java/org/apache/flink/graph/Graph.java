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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.spargel.java.MessagingFunction;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.spargel.java.VertexUpdateFunction;
import org.apache.flink.util.Collector;


@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> implements Serializable {

    private final ExecutionEnvironment context;

	private final DataSet<Tuple2<K, VV>> vertices;

	private final DataSet<Tuple3<K, K, EV>> edges;

	/** a graph is directed by default */
	private boolean isUndirected = false;
	
	private static TypeInformation<?> vertexKeyType;
	private static TypeInformation<?> vertexValueType;


	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
        this.context = context;
		Graph.vertexKeyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
		Graph.vertexValueType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(1);
	}

	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context,
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
        this.context = context;
		this.isUndirected = undirected;
		Graph.vertexKeyType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(0);
		Graph.vertexValueType = ((TupleTypeInfo<?>) vertices.getType()).getTypeAt(1);
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
			
			return new TupleTypeInfo<Tuple2<K, NV>>(vertexKeyType, newVertexValueType);
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

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges, this.context);
    }

	/**
	 * Apply value-based filtering functions to the graph
	 * and return a sub-graph that satisfies the predicates
	 * only for the vertices.
	 * @param vertexFilter
	 * @return
	 */
	public Graph<K, VV, EV> filterOnVertices(FilterFunction<VV> vertexFilter) {

		DataSet<Tuple2<K, VV>> filteredVertices = this.vertices.filter(
				new ApplyVertexFilter<K, VV>(vertexFilter));

		DataSet<Tuple3<K, K, EV>> remainingEdges = this.edges.join(filteredVertices)
				.where(0).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>())
				.join(filteredVertices).where(1).equalTo(0)
				.with(new ProjectEdge<K, VV, EV>());

		return new Graph<K, VV, EV>(filteredVertices, remainingEdges, this.context);
	}

	/**
	 * Apply value-based filtering functions to the graph
	 * and return a sub-graph that satisfies the predicates
	 * only for the edges.
	 * @param edgeFilter
	 * @return
	 */
	public Graph<K, VV, EV> filterOnEdges(FilterFunction<EV> edgeFilter) {
		DataSet<Tuple3<K, K, EV>> filteredEdges = this.edges.filter(
				new ApplyEdgeFilter<K, EV>(edgeFilter));

		return new Graph<K, VV, EV>(this.vertices, filteredEdges, this.context);
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

		return vertices.coGroup(edges).where(0).equalTo(0)
				.with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	private static final class CountNeighborsCoGroup<K, VV, EV> implements CoGroupFunction<Tuple2<K, VV>, 
		Tuple3<K, K, EV>, Tuple2<K, Long>> {
		public void coGroup(Iterable<Tuple2<K, VV>> vertex,
				Iterable<Tuple3<K, K, EV>> outEdges,
				Collector<Tuple2<K, Long>> out) {
			long count = 0;
			for (Tuple3<K, K, EV> edge : outEdges) {
				count++;
			}
			out.collect(new Tuple2<K, Long>(vertex.iterator().next().f0, count));
		}
	}
	
	/**
	 * Return the in-degree of all vertices in the graph
	 * @return A DataSet of Tuple2<vertexId, inDegree>
	 */
	public DataSet<Tuple2<K, Long>> inDegrees() {

		return vertices.coGroup(edges).where(0).equalTo(1)
				.with(new CountNeighborsCoGroup<K, VV, EV>());
	}

	/**
	 * Return the degree of all vertices in the graph
	 * @return A DataSet of Tuple2<vertexId, degree>
	 */
	public DataSet<Tuple2<K, Long>> getDegrees() {

		return outDegrees().union(inDegrees()).groupBy(0).sum(1);
	}

    private static final class VertexKeyWithOne<K, EV, VV> implements
    	MapFunction<Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>>, Tuple2<K, Long>> {

		public Tuple2<K, Long> map(
				Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>> value) {
			return new Tuple2<K, Long>(value.f0.f0, 1L);
		}
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
			return new Graph<K, VV, EV>(vertices, undirectedEdges, this.context, true);
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
			return new Graph<K, VV, EV>(vertices, (DataSet<Tuple3<K, K, EV>>) undirectedEdges, this.context, true);
		}
	}

	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV>
		create(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {
		return new Graph<K, VV, EV>(vertices, edges, context);
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
				Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass,	Class<EV> edgeValueClass,
                ExecutionEnvironment context) {

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

		return Graph.create(vertices, edges, context);
	}

    /**
     * @return Singleton DataSet containing the vertex count
     */
	public DataSet<Integer> numberOfVertices () {
        return GraphUtils.count(vertices);
    }

    /**
     *
     * @return Singleton DataSet containing the edge count
     */
	public DataSet<Integer> numberOfEdges () {
        return GraphUtils.count(edges);
    }

    /**
     *
     * @return The IDs of the vertices as DataSet
     */
    public DataSet<K> getVertexIds () {
        return vertices.map(new ExtractVertexIDMapper<K, VV>());
    }
    
    private static final class ExtractVertexIDMapper<K, VV> implements MapFunction<Tuple2<K, VV>, K> {
            @Override
            public K map(Tuple2<K, VV> vertex) throws Exception {
                return vertex.f0;
            }
    }

    public DataSet<Tuple2<K, K>> getEdgeIds () {
        return edges.map(new ExtractEdgeIDsMapper<K, EV>());
    }
    
    private static final class ExtractEdgeIDsMapper<K, EV> implements MapFunction<Tuple3<K, K, EV>, Tuple2<K, K>> {
            @Override
            public Tuple2<K, K> map(Tuple3<K, K, EV> edge) throws Exception {
                return new Tuple2<K,K>(edge.f0, edge.f1);
            }
    }

    /**
     * Checks the weak connectivity of a graph.
     * @param maxIterations the maximum number of iterations for the inner delta iteration
     * @return true if the graph is weakly connected.
     */
	public DataSet<Boolean> isWeaklyConnected (int maxIterations) {
		Graph<K, VV, EV> graph;
		
		if (!(this.isUndirected)) {
			// first, convert to an undirected graph
			graph = this.getUndirected();
		}
		else {
			graph = this;
		}

        DataSet<K> vertexIds = graph.getVertexIds();
        DataSet<Tuple2<K,K>> verticesWithInitialIds = vertexIds
                .map(new DuplicateVertexIDMapper<K>());

        DataSet<Tuple2<K,K>> edgeIds = graph.getEdgeIds();

        DeltaIteration<Tuple2<K,K>, Tuple2<K,K>> iteration = verticesWithInitialIds
                .iterateDelta(verticesWithInitialIds, maxIterations, 0);

        DataSet<Tuple2<K, K>> changes = iteration.getWorkset()
                .join(edgeIds).where(0).equalTo(0)
                .with(new FindNeighborsJoin<K>())
                .groupBy(0)
                .aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new VertexWithNewComponentJoin<K>());

        DataSet<Tuple2<K, K>> components = iteration.closeWith(changes, changes);
        DataSet<Boolean> result = GraphUtils.count(components.groupBy(1).reduceGroup(
        		new EmitFirstReducer<K>())).map(new CheckIfOneComponentMapper());	
        return result;
    }
	
	private static final class DuplicateVertexIDMapper<K> implements MapFunction<K, Tuple2<K, K>> {
            @Override
            public Tuple2<K, K> map(K k) {
                return new Tuple2<K, K>(k, k);
            }
	}
	
	private static final class FindNeighborsJoin<K> implements JoinFunction<Tuple2<K, K>, Tuple2<K, K>, 
		Tuple2<K, K>> {
        @Override
        public Tuple2<K, K> join(Tuple2<K, K> vertexWithComponent, Tuple2<K, K> edge) {
            return new Tuple2<K,K>(edge.f1, vertexWithComponent.f1);
        }
	}

	private static final class VertexWithNewComponentJoin<K extends Comparable<K>> 
		implements FlatJoinFunction<Tuple2<K, K>, Tuple2<K, K>, Tuple2<K, K>> {
        @Override
        public void join(Tuple2<K, K> candidate, Tuple2<K, K> old, Collector<Tuple2<K, K>> out) {
            if (candidate.f1.compareTo(old.f1) < 0) {
                out.collect(candidate);
            }
        }
	}
	
	private static final class EmitFirstReducer<K> implements 
		GroupReduceFunction<Tuple2<K, K>, Tuple2<K, K>> {
		public void reduce(Iterable<Tuple2<K, K>> values, Collector<Tuple2<K, K>> out) {
			out.collect(values.iterator().next());			
		}
	}
	
	private static final class CheckIfOneComponentMapper implements MapFunction<Integer, Boolean> {
        @Override
        public Boolean map(Integer n) {
        	return (n == 1);
        }
	}
	
    public Graph<K, VV, EV> fromCollection (Collection<Tuple2<K,VV>> vertices, Collection<Tuple3<K,K,EV>> edges) {

		DataSet<Tuple2<K, VV>> v = context.fromCollection(vertices);
		DataSet<Tuple3<K, K, EV>> e = context.fromCollection(edges);

		return new Graph<K, VV, EV>(v, e, context);
    }

    /**
     * Adds the input vertex and edges to the graph.
     * If the vertex already exists in the graph, it will not be added again,
     * but the given edges will. 
     * @param vertex
     * @param edges
     * @return
     */
    public Graph<K, VV, EV> addVertex (final Tuple2<K,VV> vertex, List<Tuple3<K,K,EV>> edges) {

    	DataSet<Tuple2<K, VV>> newVertex = this.context.fromCollection(Arrays.asList(vertex));

    	// Take care of empty edge set
    	if (edges.isEmpty()) {
    		return Graph.create(getVertices().union(newVertex).distinct(), getEdges(), context);
    	}

    	// Add the vertex and its edges
    	DataSet<Tuple2<K, VV>> newVertices = getVertices().union(newVertex).distinct();
    	DataSet<Tuple3<K, K, EV>> newEdges = getEdges().union(context.fromCollection(edges));

    	return Graph.create(newVertices, newEdges, context);
    }

    /** 
     * Adds the given edge to the graph.
     * If the source and target vertices do not exist in the graph,
     * they will also be added.
     * @param source
     * @param target
     * @param edgeValue
     * @return
     */
    public Graph<K, VV, EV> addEdge (Tuple2<K,VV> source, Tuple2<K,VV> target, EV edgeValue) {
    	Graph<K,VV,EV> partialGraph = this.fromCollection(Arrays.asList(source, target), 
    			Arrays.asList(new Tuple3<K, K, EV>(source.f0, target.f0, edgeValue)));
        return this.union(partialGraph);
    }

    /**
     * Removes the given vertex and its edges from the graph.
     * @param vertex
     * @return
     */
    public Graph<K, VV, EV> removeVertex (Tuple2<K,VV> vertex) {
		DataSet<Tuple2<K, VV>> newVertices = getVertices().filter(
				new RemoveVertexFilter<K, VV>(vertex));
		DataSet<Tuple3<K, K, EV>> newEdges = getEdges().filter(
				new VertexRemovalEdgeFilter<K, VV, EV>(vertex));
        return new Graph<K, VV, EV>(newVertices, newEdges, this.context);
    }
    
    private static final class RemoveVertexFilter<K, VV> implements FilterFunction<Tuple2<K, VV>> {

    	private Tuple2<K, VV> vertexToRemove;

        public RemoveVertexFilter(Tuple2<K, VV> vertex) {
        	vertexToRemove = vertex;
		}

        @Override
        public boolean filter(Tuple2<K, VV> vertex) throws Exception {
        	return !vertex.f0.equals(vertexToRemove.f0);
        }
    }
    
    private static final class VertexRemovalEdgeFilter<K, VV, EV> implements FilterFunction<Tuple3<K, K, EV>> {

    	private Tuple2<K, VV> vertexToRemove;

        public VertexRemovalEdgeFilter(Tuple2<K, VV> vertex) {
			vertexToRemove = vertex;
		}

        @Override
        public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
        	
        	if (edge.f0.equals(vertexToRemove.f0)) {
                return false;
            }
            if (edge.f1.equals(vertexToRemove.f0)) {
                return false;
            }
            return true;
        }
    }
    
    /**
     * Removes all edges that match the given edge from the graph.
     * @param edge
     * @return
     */
    public Graph<K, VV, EV> removeEdge (Tuple3<K,K,EV> edge) {
		DataSet<Tuple3<K, K, EV>> newEdges = getEdges().filter(
				new EdgeRemovalEdgeFilter<K, VV, EV>(edge));
        return new Graph<K, VV, EV>(this.getVertices(), newEdges, this.context);
    }
    
    private static final class EdgeRemovalEdgeFilter<K, VV, EV> implements FilterFunction<Tuple3<K, K, EV>> {
    	private Tuple3<K, K, EV> edgeToRemove;

        public EdgeRemovalEdgeFilter(Tuple3<K, K, EV> edge) {
			edgeToRemove = edge;
		}

        @Override
        public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
        	
    		if (edge.f0.equals(edgeToRemove.f0) 
    				&& edge.f1.equals(edgeToRemove.f1)) {
    			return false;
    		}
    		return true;
        }
    }

    /**
     * Performs union on the vertices and edges sets of the input graphs
     * removing duplicate vertices but maintaining duplicate edges.
     * @param graph
     * @return
     */
    public Graph<K, VV, EV> union (Graph<K, VV, EV> graph) {
        DataSet<Tuple2<K,VV>> unionedVertices = graph.getVertices().union(this.getVertices()).distinct();
        DataSet<Tuple3<K,K,EV>> unionedEdges = graph.getEdges().union(this.getEdges());
        return new Graph<K,VV,EV>(unionedVertices, unionedEdges, this.context);
    }

    /**
     * Runs a Vertex-Centric iteration on the graph.
     * @param vertexUpdateFunction
     * @param messagingFunction
     * @param maximumNumberOfIterations
     * @return
     */
    public <M>Graph<K, VV, EV> runVertexCentricIteration(VertexUpdateFunction<K, VV, M> vertexUpdateFunction,
    		MessagingFunction<K, VV, M, EV> messagingFunction, int maximumNumberOfIterations) {
    	DataSet<Tuple2<K, VV>> newVertices = vertices.runOperation(
    			VertexCentricIteration.withValuedEdges(edges, vertexUpdateFunction, messagingFunction, 
    			maximumNumberOfIterations));
		return new Graph<K, VV, EV>(newVertices, edges, context);
    }
}
