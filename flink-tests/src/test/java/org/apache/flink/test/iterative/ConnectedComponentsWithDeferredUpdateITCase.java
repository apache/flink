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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.graph.ConnectedComponents;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedReader;
import java.util.Arrays;
import java.util.Collection;

/**
 * Delta iteration test implementing the connected components algorithm with a cogroup and join on
 * the solution set.
 */
@RunWith(Parameterized.class)
public class ConnectedComponentsWithDeferredUpdateITCase extends JavaProgramTestBase {

    private static final long SEED = 0xBADC0FFEEBEEFL;

    private static final int NUM_VERTICES = 1000;

    private static final int NUM_EDGES = 10000;

    private final boolean extraMapper;

    protected String verticesPath;
    protected String edgesPath;
    protected String resultPath;

    public ConnectedComponentsWithDeferredUpdateITCase(boolean extraMapper) {
        this.extraMapper = extraMapper;
    }

    @Override
    protected void preSubmit() throws Exception {
        verticesPath =
                createTempFile(
                        "vertices.txt",
                        ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
        edgesPath =
                createTempFile(
                        "edges.txt",
                        ConnectedComponentsData.getRandomOddEvenEdges(
                                NUM_EDGES, NUM_VERTICES, SEED));
        resultPath = getTempFilePath("results");
    }

    @Override
    protected void testProgram() throws Exception {
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // read vertex and edge data
        DataSet<Tuple1<Long>> vertices = env.readCsvFile(verticesPath).types(Long.class);

        DataSet<Tuple2<Long, Long>> edges =
                env.readCsvFile(edgesPath)
                        .fieldDelimiter(" ")
                        .types(Long.class, Long.class)
                        .flatMap(new ConnectedComponents.UndirectEdge());

        // assign the initial components (equal to the vertex id)
        DataSet<Tuple2<Long, Long>> verticesWithInitialId =
                vertices.map(new ConnectedComponentsITCase.DuplicateValue<Long>());

        // open a delta iteration
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                verticesWithInitialId.iterateDelta(verticesWithInitialId, 100, 0);

        // apply the step logic: join with the edges, select the minimum neighbor, update if the
        // component of the candidate is smaller
        DataSet<Tuple2<Long, Long>> changes =
                iteration
                        .getWorkset()
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new ConnectedComponents.NeighborWithComponentIDJoin())
                        .groupBy(0)
                        .aggregate(Aggregations.MIN, 1)
                        .join(iteration.getSolutionSet())
                        .where(0)
                        .equalTo(0)
                        .with(new UpdateComponentIdMatchNonPreserving());

        DataSet<Tuple2<Long, Long>> delta;
        if (extraMapper) {
            delta =
                    changes.map(
                            // ID Mapper
                            new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                                private static final long serialVersionUID = -3929364091829757322L;

                                @Override
                                public Tuple2<Long, Long> map(Tuple2<Long, Long> v)
                                        throws Exception {
                                    return v;
                                }
                            });
        } else {
            delta = changes;
        }

        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple2<Long, Long>> result = iteration.closeWith(delta, changes);

        result.writeAsCsv(resultPath, "\n", " ");

        // execute program
        env.execute("Connected Components Example");
    }

    @Override
    protected void postSubmit() throws Exception {
        for (BufferedReader reader : getResultReader(resultPath)) {
            ConnectedComponentsData.checkOddEvenResult(reader);
        }
    }

    @Parameters
    public static Collection<Object[]> getConfigurations() {
        return Arrays.asList(new Object[] {false}, new Object[] {true});
    }

    private static final class UpdateComponentIdMatchNonPreserving
            implements FlatJoinFunction<
                    Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void join(
                Tuple2<Long, Long> candidate,
                Tuple2<Long, Long> current,
                Collector<Tuple2<Long, Long>> out)
                throws Exception {

            if (candidate.f1 < current.f1) {
                out.collect(candidate);
            }
        }
    }
}
