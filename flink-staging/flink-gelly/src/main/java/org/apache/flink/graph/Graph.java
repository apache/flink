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
	EV extends Serializable> implements Serializable{

	private final DataSet<Tuple2<K, VV>> vertices;

	private final DataSet<Tuple3<K, K, EV>> edges;


	/** a graph is directed by default */
	private boolean isUndirected = false;


	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges,
			boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
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

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges);
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
			DataSet<Tuple3<K, K, EV>> undirectedEdges = edges.flatMap(
					new FlatMapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {
				public void flatMap(Tuple3<K, K, EV> edge, Collector<Tuple3<K, K, EV>> out){
					out.collect(edge);
					out.collect(new Tuple3<K, K, EV>(edge.f1, edge.f0, edge.f2));
				}
			});
			return new Graph<K, VV, EV>(vertices, (DataSet<Tuple3<K, K, EV>>) undirectedEdges, true);
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

}
