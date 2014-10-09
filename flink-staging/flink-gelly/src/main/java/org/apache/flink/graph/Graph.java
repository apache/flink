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
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class Graph<K extends Comparable<K>, VV, EV> {

	private final DataSet<Vertex<K, VV>> vertices;

	private final DataSet<Edge<K, EV>> edges;



	public Graph(DataSet<Vertex<K, VV>> vertices, DataSet<Edge<K, EV>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}


	public DataSet<Vertex<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Edge<K, EV>> getEdges() {
		return edges;
	}

    // Return a Vertex Dataset or a new Graph?
    public <V2> DataSet<Vertex<K, V2>> mapVertices(final MapFunction<VV, V2> mapper) {

        return vertices.map(new MapFunction<Vertex<K, VV>, Vertex<K, V2>>() {
            @Override
            public Vertex<K, V2> map(Vertex<K, VV> kvvVertex) throws Exception {
                // Return new object for every Vertex not a good idea probably
                return  new Vertex<>(kvvVertex.getKey(), mapper.map(kvvVertex.getValue()));
            }
        });
    }


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
                    public void reduce(Iterable<Edge<K, EV>> edges, Collector<Tuple2<K, Integer>> integerCollector) throws Exception {

                        int count = 0;
                        for (Edge<K, EV> edge : edges) {
                            count++;
                        }

                        integerCollector.collect(new Tuple2<K, Integer>(edges.iterator().next().getSource(), count));
                    }
                });
    }

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
}
