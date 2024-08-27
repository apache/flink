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
 *
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Utils for building lineage graph from transformations. */
@Internal
public class LineageGraphUtils {

    /** Convert transforms to LineageGraph. */
    public static LineageGraph convertToLineageGraph(List<Transformation<?>> transformations) {
        DefaultLineageGraph.LineageGraphBuilder builder = DefaultLineageGraph.builder();
        for (Transformation<?> transformation : transformations) {
            List<LineageEdge> edges = processSink(transformation);
            for (LineageEdge lineageEdge : edges) {
                builder.addLineageEdge(lineageEdge);
            }
        }
        return builder.build();
    }

    private static List<LineageEdge> processSink(Transformation<?> transformation) {
        List<LineageEdge> lineageEdges = new ArrayList<>();
        LineageVertex sinkLineageVertex = null;
        if (transformation instanceof SinkTransformation) {
            sinkLineageVertex = ((SinkTransformation<?, ?>) transformation).getLineageVertex();
        } else if (transformation instanceof LegacySinkTransformation) {
            sinkLineageVertex = ((LegacySinkTransformation) transformation).getLineageVertex();
        }

        if (sinkLineageVertex != null) {
            List<Transformation<?>> predecessors = transformation.getTransitivePredecessors();
            for (Transformation<?> predecessor : predecessors) {
                Optional<SourceLineageVertex> sourceOpt = processSource(predecessor);
                if (sourceOpt.isPresent()) {
                    lineageEdges.add(new DefaultLineageEdge(sourceOpt.get(), sinkLineageVertex));
                }
            }
        }
        return lineageEdges;
    }

    private static Optional<SourceLineageVertex> processSource(Transformation<?> transformation) {
        if (transformation instanceof SourceTransformation) {
            if (((SourceTransformation) transformation).getLineageVertex() != null) {
                return Optional.of(
                        (SourceLineageVertex)
                                ((SourceTransformation) transformation).getLineageVertex());
            }
        } else if (transformation instanceof LegacySourceTransformation) {
            if (((LegacySourceTransformation) transformation).getLineageVertex() != null) {
                return Optional.of(
                        (SourceLineageVertex)
                                ((LegacySourceTransformation) transformation).getLineageVertex());
            }
        }
        return Optional.empty();
    }
}
