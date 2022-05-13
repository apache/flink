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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.generator.CirculantGraph.OffsetRange;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.graph.generator.CirculantGraph.MINIMUM_VERTEX_COUNT;

/** Generate a {@link org.apache.flink.graph.generator.CirculantGraph}. */
public class CirculantGraph extends GeneratedGraph<LongValue> {

    private static final String PREFIX = "range";

    private LongParameter vertexCount =
            new LongParameter(this, "vertex_count").setMinimumValue(MINIMUM_VERTEX_COUNT);

    private List<OffsetRange> offsetRanges = new ArrayList<>();

    @Override
    public String getUsage() {
        return "--"
                + PREFIX
                + "0 offset:length [--"
                + PREFIX
                + "1 offset:length [--"
                + PREFIX
                + "2 ...]] "
                + super.getUsage();
    }

    @Override
    public void configure(ParameterTool parameterTool) throws ProgramParametrizationException {
        super.configure(parameterTool);

        // add offset ranges as ordered by offset ID (range0, range1, range2, ...)

        Map<Integer, String> offsetRangeMap = new TreeMap<>();

        // first parse all offset ranges into a sorted map
        for (String key : parameterTool.toMap().keySet()) {
            if (key.startsWith(PREFIX)) {
                int offsetId = Integer.parseInt(key.substring(PREFIX.length()));
                offsetRangeMap.put(offsetId, parameterTool.get(key));
            }
        }

        // then store offset ranges in order
        for (String field : offsetRangeMap.values()) {
            ProgramParametrizationException exception =
                    new ProgramParametrizationException(
                            "Circulant offset range"
                                    + " must use a colon to separate the integer offset and integer length:"
                                    + field
                                    + "'");

            if (!field.contains(":")) {
                throw exception;
            }

            String[] parts = field.split(":");

            if (parts.length != 2) {
                throw exception;
            }

            try {
                long offset = Long.parseLong(parts[0]);
                long length = Long.parseLong(parts[1]);
                offsetRanges.add(new OffsetRange(offset, length));
            } catch (NumberFormatException ex) {
                throw exception;
            }
        }
    }

    @Override
    public String getIdentity() {
        return getName() + " (" + offsetRanges + ")";
    }

    @Override
    protected long vertexCount() {
        return vertexCount.getValue();
    }

    @Override
    public Graph<LongValue, NullValue, NullValue> create(ExecutionEnvironment env) {
        org.apache.flink.graph.generator.CirculantGraph graph =
                new org.apache.flink.graph.generator.CirculantGraph(env, vertexCount.getValue());

        for (OffsetRange offsetRange : offsetRanges) {
            graph.addRange(offsetRange.getOffset(), offsetRange.getLength());
        }

        return graph.setParallelism(parallelism.getValue().intValue()).generate();
    }
}
