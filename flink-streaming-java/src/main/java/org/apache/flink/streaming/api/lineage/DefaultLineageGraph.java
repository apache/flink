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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Default implementation for {@link LineageGraph}. */
@Internal
public class DefaultLineageGraph implements LineageGraph {
    private final List<LineageEdge> lineageEdges;
    private final List<SourceLineageVertex> sources;
    private final List<LineageVertex> sinks;

    private DefaultLineageGraph(List<LineageEdge> lineageEdges) {
        this.lineageEdges = lineageEdges;

        Set<SourceLineageVertex> deduplicatedSources = new HashSet<>();
        Set<LineageVertex> deduplicatedSinks = new HashSet<>();
        for (LineageEdge lineageEdge : lineageEdges) {
            deduplicatedSources.add(lineageEdge.source());
            deduplicatedSinks.add(lineageEdge.sink());
        }
        this.sources = new ArrayList<>(deduplicatedSources);
        this.sinks = new ArrayList<>(deduplicatedSinks);
    }

    @Override
    public List<SourceLineageVertex> sources() {
        return Collections.unmodifiableList(sources);
    }

    @Override
    public List<LineageVertex> sinks() {
        return Collections.unmodifiableList(sinks);
    }

    @Override
    public List<LineageEdge> relations() {
        return Collections.unmodifiableList(lineageEdges);
    }

    public static LineageGraphBuilder builder() {
        return new LineageGraphBuilder();
    }

    /** Build the default lineage graph from {@link LineageEdge}. */
    @Internal
    public static class LineageGraphBuilder {
        private final List<LineageEdge> lineageEdges;

        private LineageGraphBuilder() {
            this.lineageEdges = new ArrayList<>();
        }

        public LineageGraphBuilder addLineageEdge(LineageEdge lineageEdge) {
            this.lineageEdges.add(lineageEdge);
            return this;
        }

        public LineageGraphBuilder addLineageEdges(LineageEdge... lineageEdges) {
            this.lineageEdges.addAll(Arrays.asList(lineageEdges));
            return this;
        }

        public LineageGraph build() {
            return new DefaultLineageGraph(lineageEdges);
        }
    }
}
