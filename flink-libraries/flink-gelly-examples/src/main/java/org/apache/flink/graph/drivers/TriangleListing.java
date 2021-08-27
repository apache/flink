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

package org.apache.flink.graph.drivers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.types.CopyableValue;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;

import java.io.PrintStream;

/**
 * Driver for directed and undirected triangle listing algorithm and analytic.
 *
 * @see org.apache.flink.graph.library.clustering.directed.TriangleListing
 * @see org.apache.flink.graph.library.clustering.directed.TriadicCensus
 * @see org.apache.flink.graph.library.clustering.undirected.TriangleListing
 * @see org.apache.flink.graph.library.clustering.undirected.TriadicCensus
 */
public class TriangleListing<K extends Comparable<K> & CopyableValue<K>, VV, EV>
        extends DriverBase<K, VV, EV> {

    private static final String DIRECTED = "directed";

    private static final String UNDIRECTED = "undirected";

    private ChoiceParameter order =
            new ChoiceParameter(this, "order").addChoices(DIRECTED, UNDIRECTED);

    private BooleanParameter sortTriangleVertices =
            new BooleanParameter(this, "sort_triangle_vertices");

    private BooleanParameter computeTriadicCensus = new BooleanParameter(this, "triadic_census");

    private BooleanParameter permuteResults = new BooleanParameter(this, "permute_results");

    private GraphAnalytic<K, VV, EV, ? extends PrintableResult> triadicCensus;

    @Override
    public String getShortDescription() {
        return "list triangles";
    }

    @Override
    public String getLongDescription() {
        return WordUtils.wrap(
                new StrBuilder()
                        .appendln("List all triangles graph.")
                        .appendNewLine()
                        .append(
                                "The algorithm result contains three vertex IDs. For the directed algorithm "
                                        + "the result contains an additional bitmask indicating the presence of the six "
                                        + "potential connecting edges.")
                        .toString(),
                80);
    }

    @Override
    public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
        int parallelism = this.parallelism.getValue().intValue();

        switch (order.getValue()) {
            case DIRECTED:
                if (computeTriadicCensus.getValue()) {
                    triadicCensus =
                            graph.run(
                                    new org.apache.flink.graph.library.clustering.directed
                                                            .TriadicCensus<
                                                    K, VV, EV>()
                                            .setParallelism(parallelism));
                }

                @SuppressWarnings("unchecked")
                DataSet<PrintableResult> directedResult =
                        (DataSet<PrintableResult>)
                                (DataSet<?>)
                                        graph.run(
                                                new org.apache.flink.graph.library.clustering
                                                                        .directed.TriangleListing<
                                                                K, VV, EV>()
                                                        .setPermuteResults(
                                                                permuteResults.getValue())
                                                        .setSortTriangleVertices(
                                                                sortTriangleVertices.getValue())
                                                        .setParallelism(parallelism));
                return directedResult;

            case UNDIRECTED:
                if (computeTriadicCensus.getValue()) {
                    triadicCensus =
                            graph.run(
                                    new org.apache.flink.graph.library.clustering.undirected
                                                            .TriadicCensus<
                                                    K, VV, EV>()
                                            .setParallelism(parallelism));
                }

                @SuppressWarnings("unchecked")
                DataSet<PrintableResult> undirectedResult =
                        (DataSet<PrintableResult>)
                                (DataSet<?>)
                                        graph.run(
                                                new org.apache.flink.graph.library.clustering
                                                                        .undirected.TriangleListing<
                                                                K, VV, EV>()
                                                        .setPermuteResults(
                                                                permuteResults.getValue())
                                                        .setSortTriangleVertices(
                                                                sortTriangleVertices.getValue())
                                                        .setParallelism(parallelism));
                return undirectedResult;

            default:
                throw new RuntimeException("Unknown order: " + order);
        }
    }

    @Override
    public void printAnalytics(PrintStream out) {
        if (computeTriadicCensus.getValue()) {
            out.print("Triadic census:\n  ");
            out.println(triadicCensus.getResult().toPrintableString().replace(";", "\n "));
        }
    }
}
