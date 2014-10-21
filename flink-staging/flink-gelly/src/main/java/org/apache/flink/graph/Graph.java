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
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.spargel.java.VertexCentricIteration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> implements Serializable{

    private final ExecutionEnvironment context;

	private final DataSet<Tuple2<K, VV>> vertices;

	private final DataSet<Tuple3<K, K, EV>> edges;


	/** a graph is directed by default */
	private boolean isUndirected = false;


	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context) {
		this.vertices = vertices;
		this.edges = edges;
        this.context = context;
	}

	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, ExecutionEnvironment context,
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
        this.context = context;
		this.isUndirected = undirected;
	}

	public DataSet<Tuple2<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Tuple3<K, K, EV>> getEdges() {
		return edges;
	}

    /**
     * Apply a function to the attribute of each Tuple2 in the graph
     * @param mapper A function that transforms the attribute of each Tuple2
     * @return A DataSet of Tuple2 which contains the new values of all vertices
     */
    //TODO(thvasilo): Make it possible for the function to change the attribute type
    public DataSet<Tuple2<K, VV>> mapVertices(final MapFunction<VV, VV> mapper) {
        // Return a Tuple2 Dataset or a new Graph?
        return vertices.map(new MapFunction<Tuple2<K, VV>, Tuple2<K, VV>>() {
            @Override
            public Tuple2<K, VV> map(Tuple2<K, VV> kvvTuple2) throws Exception {
                // Return new object for every Tuple2 not a good idea probably
                return new Tuple2<>(kvvTuple2.f0, mapper.map(kvvTuple2.f1));
            }
        });
    }

    /**
     * Apply filtering functions to the graph and return a sub-graph that satisfies
     * the predicates
     * @param Tuple2Filter
     * @param edgeFilter
     * @return
     */
    // TODO(thvasilo): Add proper edge filtering functionality
    public Graph<K, VV, EV> subgraph(final FilterFunction<VV> Tuple2Filter, final FilterFunction<EV> edgeFilter) {

        DataSet<Tuple2<K, VV>> filteredVertices = this.vertices.filter(new FilterFunction<Tuple2<K, VV>>() {
            @Override
            public boolean filter(Tuple2<K, VV> kvvTuple2) throws Exception {
                return Tuple2Filter.filter(kvvTuple2.f1);
            }
        });

        // Should combine with Tuple2 filter function as well, so that only
        // edges that satisfy edge filter *and* connect vertices that satisfy Tuple2
        // filter are returned
        DataSet<Tuple3<K, K, EV>> filteredEdges = this.edges.filter(new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> kevEdge) throws Exception {
                return edgeFilter.filter(kevEdge.f2);
            }
        });

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges, this.context);
    }


    /**
     * Return the out-degree of all vertices in the graph
     * @return A DataSet of Tuple2 containing the out-degrees of the vertices in the graph
     */
    public DataSet<Tuple2<K, Integer>> outDegrees() {
        return this.edges
                .groupBy(new KeySelector<Tuple3<K, K, EV>, K>() {
                    @Override
                    public K getKey(Tuple3<K, K, EV> kevEdge) throws Exception {
                        return kevEdge.f0;
                    }
                })
                .reduceGroup(new GroupReduceFunction<Tuple3<K, K, EV>, Tuple2<K, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<K, K, EV>> edges, Collector<Tuple2<K, Integer>> integerCollector)
                            throws Exception {

                        int count = 0;
                        for (Tuple3<K, K, EV> edge : edges) {
                            count++;
                        }

                        integerCollector.collect(new Tuple2<K, Integer>(edges.iterator().next().f0, count));
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

        return new Graph<>(result, this.edges, this.context);
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
			DataSet<Tuple3<K, K, EV>> undirectedEdges = edges.flatMap(
					new FlatMapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {
				public void flatMap(Tuple3<K, K, EV> edge, Collector<Tuple3<K, K, EV>> out){
					out.collect(edge);
					out.collect(new Tuple3<K, K, EV>(edge.f1, edge.f0, edge.f2));
				}
			});
			return new Graph<K, VV, EV>(vertices, (DataSet<Tuple3<K, K, EV>>) undirectedEdges, this.context, true);
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
			DataSet<Tuple3<K, K, EV>> undirectedEdges = edges.map(new MapFunction<Tuple3<K, K, EV>,
					Tuple3<K, K, EV>>() {
				public Tuple3<K, K, EV> map(Tuple3<K, K, EV> edge){
					return new Tuple3<K, K, EV>(edge.f1, edge.f0, edge.f2);
				}
			});
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
        return GraphUtils.count((DataSet<Object>) (DataSet<?>) vertices);

    }

    /**
     *
     * @return Singleton DataSet containing the edge count
     */
    public DataSet<Integer> numberOfEdges () {
        return GraphUtils.count((DataSet<Object>) (DataSet<?>) edges);
    }


    /**
     *
     * @return The IDs of the vertices as DataSet
     */
    public DataSet<K> getVertexIds () {
        return vertices.map(new MapFunction<Tuple2<K, VV>, K>() {
            @Override
            public K map(Tuple2<K, VV> vertex) throws Exception {
                return vertex.f0;
            }
        });
    }

    public DataSet<Tuple2<K,K>> getEdgeIds () {
        return edges.map(new MapFunction<Tuple3<K, K, EV>, Tuple2<K, K>>() {
            @Override
            public Tuple2<K, K> map(Tuple3<K, K, EV> edge) throws Exception {
                return new Tuple2<K,K>(edge.f0, edge.f1);
            }
        });
    }

    public DataSet<Boolean> isWeaklyConnected () {

        DataSet<K> vertexIds = this.getVertexIds();
        DataSet<Tuple2<K,K>> verticesWithInitialIds = vertexIds
                .map(new MapFunction<K, Tuple2<K, K>>() {
                    @Override
                    public Tuple2<K, K> map(K k) throws Exception {
                        return new Tuple2<K, K>(k, k);
                    }
                });

        DataSet<Tuple2<K,K>> edgeIds = this.getEdgeIds();

        DeltaIteration<Tuple2<K,K>, Tuple2<K,K>> iteration = verticesWithInitialIds
                .iterateDelta(verticesWithInitialIds, 10, 0);

        DataSet<Tuple2<K, K>> changes = iteration.getWorkset()
                .join(edgeIds).where(0).equalTo(0)
                .with(new JoinFunction<Tuple2<K, K>, Tuple2<K, K>, Tuple2<K, K>>() {
                    @Override
                    public Tuple2<K, K> join(Tuple2<K, K> vertexWithComponent, Tuple2<K, K> edge) throws Exception {
                        return new Tuple2<K,K>(edge.f1, vertexWithComponent.f1);
                    }
                })
                .groupBy(0)
                .aggregate(Aggregations.MIN, 1)
                .join(iteration.getSolutionSet()).where(0).equalTo(0)
                .with(new FlatJoinFunction<Tuple2<K, K>, Tuple2<K, K>, Tuple2<K, K>>() {
                    @Override
                    public void join(Tuple2<K, K> candidate, Tuple2<K, K> old, Collector<Tuple2<K, K>> out) throws Exception {
                        if (candidate.f1.compareTo(old.f1) < 0) {
                            out.collect(candidate);
                        }
                    }
                });

        DataSet<Tuple2<K, K>> components = iteration.closeWith(changes, changes);

        DataSet<Boolean> result = GraphUtils.count((DataSet<Object>) (DataSet<?>) components)
                .map(new MapFunction<Integer, Boolean>() {
                    @Override
                    public Boolean map(Integer n) throws Exception {
                        if (n == 1)
                            return false;
                        else
                            return true;
                    }
                });

        return result;
    }

    //TODO kostas add functionality
    public Graph<K, VV, EV> fromCollection (Collection<Tuple2<K,VV>> vertices, Collection<Tuple3<K,K,EV>> edges) {
        return null;
    }

    //TODO kostas add functionality
    public DataSet<Tuple2<K, VV>> fromCollection (Collection<Tuple2<K,VV>> vertices) {
        return null;
    }


    public Graph<K, VV, EV> addVertex (Tuple2<K,VV> vertex, List<Tuple3<K,K,EV>> edges) {
        Graph<K,VV,EV> newVertex = this.fromCollection(Arrays.asList(vertex), edges);
        return this.union(newVertex);
    }

    public Graph<K, VV, EV> removeVertex (Tuple2<K,VV> vertex) {

        DataSet<Tuple2<K,VV>> vertexToRemove = fromCollection(Arrays.asList(vertex));

        DataSet<Tuple2<K,VV>> newVertices = getVertices()
                .filter(new RichFilterFunction<Tuple2<K, VV>>() {
                    private Tuple2<K, VV> vertexToRemove;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.vertexToRemove = (Tuple2<K, VV>) getRuntimeContext().getBroadcastVariable("vertexToRemove").get(0);
                    }

                    @Override
                    public boolean filter(Tuple2<K, VV> vertex) throws Exception {
                        if (vertex.f0.equals(vertexToRemove.f0)) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                }).withBroadcastSet(vertexToRemove, "vertexToRemove");

        DataSet<Tuple3<K,K,EV>> newEdges = getEdges()
                .filter(new RichFilterFunction<Tuple3<K,K,EV>>() {
                    private Tuple2<K, VV> vertexToRemove;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.vertexToRemove = (Tuple2<K, VV>) getRuntimeContext().getBroadcastVariable("vertexToRemove").get(0);
                    }

                    @Override
                    public boolean filter(Tuple3<K,K,EV> edge) throws Exception {
                        if (edge.f0.equals(vertexToRemove.f0)) {
                            return false;
                        }
                        if (edge.f1.equals(vertexToRemove.f0)) {
                            return false;
                        }
                        return true;
                    }
                }).withBroadcastSet(vertexToRemove, "vertexToRemove");

        return new Graph<K, VV, EV>(newVertices, newEdges, this.context);
    }


    public Graph<K, VV, EV> addEdge (Tuple3<K,K,EV> edge, Tuple2<K,VV> source, Tuple2<K,VV> target) {
        Graph<K,VV,EV> newEdges = this.fromCollection(Arrays.asList(source, target), Arrays.asList(edge));
        return this.union(newEdges);
    }

    public Graph<K, VV, EV> union (Graph<K, VV, EV> graph) {
        DataSet<Tuple2<K,VV>> unionedVertices = graph.getVertices().union(this.getVertices());
        DataSet<Tuple3<K,K,EV>> unionedEdges = graph.getEdges().union(this.getEdges());
        return new Graph<K,VV,EV>(unionedVertices, unionedEdges, this.context);
    }


    public Graph<K, VV, EV> passMessages (VertexCentricIteration<K, VV, ?, EV> iteration) {
        DataSet<Tuple2<K,VV>> newVertices = iteration.createResult();
        return new Graph<K,VV,EV>(newVertices, edges, this.context);
    }


}
