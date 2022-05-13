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
import org.apache.flink.graph.asm.dataset.ChecksumHashCode.Checksum;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests for {@link ClusteringCoefficient}. */
@RunWith(Parameterized.class)
public class ClusteringCoefficientITCase extends CopyableValueDriverBaseITCase {

    public ClusteringCoefficientITCase(String idType, TestExecutionMode mode) {
        super(idType, mode);
    }

    private String[] parameters(int scale, String order, String simplify, String output) {
        return new String[] {
            "--algorithm",
            "ClusteringCoefficient",
            "--order",
            order,
            "--input",
            "RMatGraph",
            "--scale",
            Integer.toString(scale),
            "--type",
            idType,
            "--simplify",
            simplify,
            "--output",
            output
        };
    }

    @Test
    public void testLongDescription() throws Exception {
        String expected = regexSubstring(new ClusteringCoefficient().getLongDescription());

        expectedOutputFromException(
                new String[] {"--algorithm", "ClusteringCoefficient"},
                expected,
                ProgramParametrizationException.class);
    }

    @Test
    public void testHashWithDirectedRMatGraph() throws Exception {
        String expected =
                "\n"
                        + new Checksum(233, 0x00000075d99a55b6L)
                        + "\n\n"
                        + "triplet count: 97315, triangle count: 30705, global clustering coefficient: 0.31552175[0-9]+\n"
                        + "vertex count: 233, average clustering coefficient: 0.41178524[0-9]+\n";

        expectedOutput(parameters(8, "directed", "directed", "hash"), expected);
    }

    @Test
    public void testHashWithUndirectedRMatGraph() throws Exception {
        String expected =
                "\n\n"
                        + "triplet count: 97315, triangle count: 30705, global clustering coefficient: 0.31552175[0-9]+\n"
                        + "vertex count: 233, average clustering coefficient: 0.50945459[0-9]+\n";

        expectedOutput(
                parameters(8, "directed", "undirected", "hash"),
                "\n" + new Checksum(233, 0x00000076635e00e2L) + expected);
        expectedOutput(
                parameters(8, "undirected", "undirected", "hash"),
                "\n" + new Checksum(233, 0x000000743ef6d14bL) + expected);
    }

    @Test
    public void testParallelism() throws Exception {
        String[] largeOperators =
                new String[] {
                    "Combine \\(Count triangles\\)",
                    "FlatMap \\(Split triangle vertices\\)",
                    "Join \\(Triangle listing\\)",
                    "GroupReduce \\(Generate triplets\\)",
                    "DataSink \\(Count\\)"
                };

        TestUtils.verifyParallelism(parameters(8, "directed", "directed", "print"), largeOperators);
        TestUtils.verifyParallelism(
                parameters(8, "directed", "undirected", "print"), largeOperators);
        TestUtils.verifyParallelism(
                parameters(8, "undirected", "undirected", "print"), largeOperators);
    }
}
