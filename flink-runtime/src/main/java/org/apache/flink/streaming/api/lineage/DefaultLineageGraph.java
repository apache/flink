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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Default implementation for {@link LineageGraph}. */
@Internal
public class DefaultLineageGraph implements LineageGraph {
    @JsonProperty private final List<LineageEdge> lineageEdges;
    @JsonProperty private final Set<SourceLineageVertex> sources;
    @JsonProperty private final Set<LineageVertex> sinks;

    private DefaultLineageGraph(List<LineageEdge> lineageEdges) {
        this.lineageEdges = lineageEdges;
        this.sources = new HashSet<>();
        this.sinks = new HashSet<>();
        for (LineageEdge lineageEdge : lineageEdges) {
            sources.add(lineageEdge.source());
            sinks.add(lineageEdge.sink());
        }
    }

    @Override
    public List<SourceLineageVertex> sources() {
        return List.copyOf(sources);
    }

    @Override
    public List<LineageVertex> sinks() {
        return List.copyOf(sinks);
    }

    @Override
    public List<LineageEdge> relations() {
        return Collections.unmodifiableList(lineageEdges);
    }

    void addSources(List<SourceLineageVertex> partialSources) {
        this.sources.addAll(partialSources);
    }

    void addSinks(List<LineageVertex> partialSinks) {
        this.sinks.addAll(partialSinks);
    }

    public static LineageGraphBuilder builder() {
        return new LineageGraphBuilder();
    }

    /** Build the default lineage graph from {@link LineageEdge}. */
    @Internal
    public static class LineageGraphBuilder {
        private final List<LineageEdge> lineageEdges;
        private final List<SourceLineageVertex> sources;
        private final List<LineageVertex> sinks;

        private LineageGraphBuilder() {
            this.lineageEdges = new ArrayList<>();
            this.sources = new ArrayList<>();
            this.sinks = new ArrayList<>();
        }

        public LineageGraphBuilder addLineageEdge(LineageEdge lineageEdge) {
            this.lineageEdges.add(lineageEdge);
            return this;
        }

        public LineageGraphBuilder addLineageEdges(LineageEdge... lineageEdges) {
            this.lineageEdges.addAll(Arrays.asList(lineageEdges));
            return this;
        }

        public LineageGraphBuilder addSourceVertex(SourceLineageVertex sourceVertex) {
            this.sources.add(sourceVertex);
            return this;
        }

        public LineageGraphBuilder addSourceVertexes(SourceLineageVertex... sourceVertexes) {
            this.sources.addAll(Arrays.asList(sourceVertexes));
            return this;
        }

        public LineageGraphBuilder addSinkVertex(LineageVertex sinkVertex) {
            this.sinks.add(sinkVertex);
            return this;
        }

        public LineageGraphBuilder addSinkVertexes(List<LineageVertex> sinkVertex) {
            this.sinks.addAll(sinkVertex);
            return this;
        }

        public LineageGraph build() {
            DefaultLineageGraph lineageGraph = new DefaultLineageGraph(lineageEdges);
            lineageGraph.addSinks(sinks);
            lineageGraph.addSources(sources);
            return lineageGraph;
        }
    }
}
