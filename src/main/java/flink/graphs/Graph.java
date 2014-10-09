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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import java.io.Serializable;


@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable>{

	private final DataSet<Vertex<K, VV>> vertices;

	private final DataSet<Edge<K, EV>> edges;


	/** a graph is directed by default */
	private boolean isUndirected = false;


	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges,
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
		this.isUndirected = undirected;
	}

	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}

    /**
     * Apply a function to the attribute of each vertex in the graph
     * @param mapper A function that transforms the attribute of each vertex
     * @param <V2> The type of the vertex attribute after the function has been applied
     * @return A DataSet of Vertex which contains the new values of all vertices
     */
    public <V2 extends Serializable> DataSet<Vertex<K, V2>> mapVertices(final MapFunction<VV, V2> mapper) {
        // Return a Vertex Dataset or a new Graph?
        return vertices.map(new MapFunction<Vertex<K, VV>, Vertex<K, V2>>() {
            @Override
            public Vertex<K, V2> map(Vertex<K, VV> kvvVertex) throws Exception {
                // Return new object for every Vertex not a good idea probably
                return  new Vertex<>(kvvVertex.getId(), mapper.map(kvvVertex.getValue()));
            }
        });
    }


    /**
     * Apply filtering functions to the graph and return a sub-graph that satisfies
     * the predicates
     * @param vertexFilter
     * @param edgeFilter
     * @return
     */
    // TODO(thvasilo): Add proper edge filtering functionality
    public Graph<K, VV, EV> subgraph(final FilterFunction<VV> vertexFilter, final FilterFunction<EV> edgeFilter) {

        DataSet<Vertex<K, VV>> filteredVertices = this.vertices.filter(new FilterFunction<Vertex<K, VV>>() {
            @Override
            public boolean filter(Vertex<K, VV> kvvVertex) throws Exception {
                return vertexFilter.filter(kvvVertex.getValue());
            }
        });

        // Should combine with vertex filter function as well, so that only
        // edges that satisfy edge filter *and* connect vertices that satisfy vertex
        // filter are returned
        DataSet<Edge<K, EV>> filteredEdges = this.edges.filter(new FilterFunction<Edge<K, EV>>() {
            @Override
            public boolean filter(Edge<K, EV> kevEdge) throws Exception {
                return edgeFilter.filter(kevEdge.getValue());
            }
        });

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges);
    }


    /**
     * Return the out-degree of all vertices in the graph
     * @return A DataSet of Tuple2 containing the out-degrees of the vertices in the graph
     */
    public DataSet<Tuple2<K, Integer>> outDegrees() {
        return this.edges
                .groupBy(new KeySelector<Edge<K, EV>, K>() {
                    @Override
                    public K getKey(Edge<K, EV> kevEdge) throws Exception {
                        return kevEdge.getSource();
                    }
                })
                .reduceGroup(new GroupReduceFunction<Edge<K, EV>, Tuple2<K, Integer>>() {
                    @Override
                    public void reduce(Iterable<Edge<K, EV>> edges, Collector<Tuple2<K, Integer>> integerCollector)
                            throws Exception {

                        int count = 0;
                        for (Edge<K, EV> edge : edges) {
                            count++;
                        }

                        integerCollector.collect(new Tuple2<K, Integer>(edges.iterator().next().getSource(), count));
                    }
                });
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
    public <MsgT> Graph<K, VV, EV> pga(CoGroupFunction<Vertex<K, VV>, Edge<K, EV>, Tuple2<K, MsgT>> cog,
                                       GroupReduceFunction<Tuple2<K, MsgT>, Tuple2<K, MsgT>> gred,
                                       FlatJoinFunction<Tuple2<K, MsgT>, Vertex<K, VV>, Vertex<K, VV>> fjoin,
                                       int maxIterations){

        DeltaIteration<Vertex<K, VV>, Vertex<K, VV>> iteration = this.vertices
            .iterateDelta(this.vertices, maxIterations, 0);

        DataSet<Tuple2<K, MsgT>> p = iteration.getWorkset().coGroup(this.edges).where(0).equalTo(0).with(cog);

        DataSet<Tuple2<K, MsgT>> g = p.groupBy(0).reduceGroup(gred);

        DataSet<Vertex<K, VV>> a = g.join(iteration.getSolutionSet()).where(0).equalTo(0).with(fjoin);

        DataSet<Vertex<K, VV>> result = iteration.closeWith(a, a);

        return new Graph<>(result, this.edges);
    }

	/**
	 * Convert the directed graph into an undirected graph
	 * by adding all inverse-direction edges.
	 *
	 */
	public Graph<K, VV, EV> getUndirected() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("");
		}
		else {
			DataSet<Edge<K,EV>> undirectedEdges = edges.flatMap(
					new FlatMapFunction<Edge<K,EV>, Edge<K,EV>>() {
				public void flatMap(Edge<K,EV> edge, Collector<Edge<K,EV>> out){
					out.collect(edge);
					out.collect(edge.reverse());
				}
			});
			return new Graph<K, VV, EV>(vertices, (DataSet<Edge<K, EV>>) undirectedEdges, true);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("");
		}
		else {
			DataSet<Edge<K, EV>> undirectedEdges = edges.map(new MapFunction<Edge<K, EV>,
					Edge<K, EV>>() {
				public Edge<K, EV> map(Edge<K, EV> edge){
					return edge.reverse();
				}
			});
			return new Graph<K, VV, EV>(vertices, (DataSet<Edge<K, EV>>) undirectedEdges, true);
		}
	}

	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV>
		create(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges) {
		return new Graph<K, VV, EV>(vertices, edges);

	}

	/**
	 * Read and create the graph vertex dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param vertexIdClass
	 * @param vertexValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable>
		DataSet<Vertex<K, VV>> readVertexCsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> vertexIdClass, Class<VV> vertexValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Vertex<K, VV>> vertices = reader.fieldDelimiter(delimiter).types(vertexIdClass, vertexValueClass)
		.map(new MapFunction<Tuple2<K, VV>, Vertex<K, VV>>() {

			public Vertex<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Vertex<K, VV>)value;
			}
		});
		return vertices;
	}

	/**
	 * Read and create the graph edge dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param vertexIdClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, EV extends Serializable>
		DataSet<Edge<K, EV>> readEdgesCsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> vertexIdClass, Class<EV> edgeValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Edge<K, EV>> edges = reader.fieldDelimiter(delimiter)
			.types(vertexIdClass, vertexIdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Edge<K, EV>>() {

			public Edge<K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Edge<K, EV>)value;
			}
		});
		return edges;
	}

	/**
	 * Create the graph, by reading a csv file for vertices
	 * and a csv file for the edges
	 * @param env
	 * @param vertexFilepath
	 * @param vertexDelimiter
	 * @param edgeFilepath
	 * @param edgeDelimiter
	 * @param vertexIdClass
	 * @param vertexValueClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV> readGraphFromCsvFile(ExecutionEnvironment env,
				String vertexFilepath, char vertexDelimiter, String edgeFilepath, char edgeDelimiter,
				Class<K> vertexIdClass, Class<VV> vertexValueClass,	Class<EV> edgeValueClass) {

		CsvReader vertexReader = new CsvReader(vertexFilepath, env);
		DataSet<Vertex<K, VV>> vertices = vertexReader.fieldDelimiter(vertexDelimiter)
				.types(vertexIdClass, vertexValueClass).map(new MapFunction<Tuple2<K, VV>,
						Vertex<K, VV>>() {

			public Vertex<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Vertex<K, VV>)value;
			}
		});

		CsvReader edgeReader = new CsvReader(edgeFilepath, env);
		DataSet<Edge<K, EV>> edges = edgeReader.fieldDelimiter(edgeDelimiter)
			.types(vertexIdClass, vertexIdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Edge<K, EV>>() {

			public Edge<K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Edge<K, EV>)value;
			}
		});

		return Graph.create(vertices, edges);
	}

}
