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

import org.apache.flink.client.program.ProgramParametrizationException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests for {@link GraphMetrics}. */
@RunWith(Parameterized.class)
public class GraphMetricsITCase extends DriverBaseITCase {

    public GraphMetricsITCase(String idType, TestExecutionMode mode) {
        super(idType, mode);
    }

    private String[] parameters(int scale, String order, String output) {
        return new String[] {
            "--algorithm",
            "GraphMetrics",
            "--order",
            order,
            "--input",
            "RMatGraph",
            "--scale",
            Integer.toString(scale),
            "--type",
            idType,
            "--simplify",
            order,
            "--output",
            output
        };
    }

    @Test
    public void testLongDescription() throws Exception {
        String expected = regexSubstring(new GraphMetrics().getLongDescription());

        expectedOutputFromException(
                new String[] {"--algorithm", "GraphMetrics"},
                expected,
                ProgramParametrizationException.class);
    }

    @Test
    public void testWithDirectedRMatGraph() throws Exception {
        String expected =
                "\n"
                        + "Vertex metrics:\n"
                        + "  vertex count: 117\n"
                        + "  edge count: 1,168\n"
                        + "  unidirectional edge count: 686\n"
                        + "  bidirectional edge count: 241\n"
                        + "  average degree: 9.983\n"
                        + "  density: 0.08605953\n"
                        + "  triplet count: 29,286\n"
                        + "  maximum degree: 91\n"
                        + "  maximum out degree: 77\n"
                        + "  maximum in degree: 68\n"
                        + "  maximum triplets: 4,095\n"
                        + "\n"
                        + "Edge metrics:\n"
                        + "  triangle triplet count: 4,575\n"
                        + "  rectangle triplet count: 11,756\n"
                        + "  maximum triangle triplets: 153\n"
                        + "  maximum rectangle triplets: 391\n";

        expectedOutput(parameters(7, "directed", "hash"), expected);
        expectedOutput(parameters(7, "directed", "print"), expected);
    }

    @Test
    public void testWithUndirectedRMatGraph() throws Exception {
        String expected =
                "\n"
                        + "Vertex metrics:\n"
                        + "  vertex count: 117\n"
                        + "  edge count: 927\n"
                        + "  average degree: 15.846\n"
                        + "  density: 0.13660477\n"
                        + "  triplet count: 29,286\n"
                        + "  maximum degree: 91\n"
                        + "  maximum triplets: 4,095\n"
                        + "\n"
                        + "Edge metrics:\n"
                        + "  triangle triplet count: 4,575\n"
                        + "  rectangle triplet count: 11,756\n"
                        + "  maximum triangle triplets: 153\n"
                        + "  maximum rectangle triplets: 391\n";

        expectedOutput(parameters(7, "undirected", "hash"), expected);
        expectedOutput(parameters(7, "undirected", "print"), expected);
    }

    @Test
    public void testParallelism() throws Exception {
        TestUtils.verifyParallelism(parameters(8, "directed", "print"));
        TestUtils.verifyParallelism(parameters(8, "undirected", "print"));
    }
}
