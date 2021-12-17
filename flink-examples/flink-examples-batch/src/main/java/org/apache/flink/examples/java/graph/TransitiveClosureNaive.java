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

package org.apache.flink.examples.java.graph;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * The transitive closure of a graph contains an edge for each pair of vertices which are endpoints
 * of at least one path in the graph.
 *
 * <p>This algorithm is implemented using a delta iteration. The transitive closure solution set is
 * grown in each step by joining the workset of newly discovered path endpoints with the original
 * graph edges and discarding previously discovered path endpoints (already in the solution set).
 */
@SuppressWarnings("serial")
public class TransitiveClosureNaive {

    public static void main(String... args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final int maxIterations = params.getInt("iterations", 10);

        DataSet<Tuple2<Long, Long>> edges;
        if (params.has("edges")) {
            edges =
                    env.readCsvFile(params.get("edges"))
                            .fieldDelimiter(" ")
                            .types(Long.class, Long.class);
        } else {
            System.out.println(
                    "Executing TransitiveClosureNaive example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            edges = ConnectedComponentsData.getDefaultEdgeDataSet(env);
        }

        IterativeDataSet<Tuple2<Long, Long>> paths = edges.iterate(maxIterations);

        DataSet<Tuple2<Long, Long>> nextPaths =
                paths.join(edges)
                        .where(1)
                        .equalTo(0)
                        .with(
                                new JoinFunction<
                                        Tuple2<Long, Long>,
                                        Tuple2<Long, Long>,
                                        Tuple2<Long, Long>>() {
                                    @Override
                                    /**
                                     * left: Path (z,x) - x is reachable by z right: Edge (x,y) -
                                     * edge x-->y exists out: Path (z,y) - y is reachable by z
                                     */
                                    public Tuple2<Long, Long> join(
                                            Tuple2<Long, Long> left, Tuple2<Long, Long> right)
                                            throws Exception {
                                        return new Tuple2<Long, Long>(left.f0, right.f1);
                                    }
                                })
                        .withForwardedFieldsFirst("0")
                        .withForwardedFieldsSecond("1")
                        .union(paths)
                        .groupBy(0, 1)
                        .reduceGroup(
                                new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                    @Override
                                    public void reduce(
                                            Iterable<Tuple2<Long, Long>> values,
                                            Collector<Tuple2<Long, Long>> out)
                                            throws Exception {
                                        out.collect(values.iterator().next());
                                    }
                                })
                        .withForwardedFields("0;1");

        DataSet<Tuple2<Long, Long>> newPaths =
                paths.coGroup(nextPaths)
                        .where(0)
                        .equalTo(0)
                        .with(
                                new CoGroupFunction<
                                        Tuple2<Long, Long>,
                                        Tuple2<Long, Long>,
                                        Tuple2<Long, Long>>() {
                                    Set<Tuple2<Long, Long>> prevSet =
                                            new HashSet<Tuple2<Long, Long>>();

                                    @Override
                                    public void coGroup(
                                            Iterable<Tuple2<Long, Long>> prevPaths,
                                            Iterable<Tuple2<Long, Long>> nextPaths,
                                            Collector<Tuple2<Long, Long>> out)
                                            throws Exception {
                                        for (Tuple2<Long, Long> prev : prevPaths) {
                                            prevSet.add(prev);
                                        }
                                        for (Tuple2<Long, Long> next : nextPaths) {
                                            if (!prevSet.contains(next)) {
                                                out.collect(next);
                                            }
                                        }
                                    }
                                })
                        .withForwardedFieldsFirst("0")
                        .withForwardedFieldsSecond("0");

        DataSet<Tuple2<Long, Long>> transitiveClosure = paths.closeWith(nextPaths, newPaths);

        // emit result
        if (params.has("output")) {
            transitiveClosure.writeAsCsv(params.get("output"), "\n", " ");

            // execute program explicitly, because file sinks are lazy
            env.execute("Transitive Closure Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            transitiveClosure.print();
        }
    }
}
