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

import org.apache.flink.api.connector.source.Boundedness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Testing for lineage graph. */
class DefaultLineageGraphTest {
    @Test
    void testLineageGraph() {
        SourceLineageVertex source1 = new TestingSourceLineageVertex("source1");
        SourceLineageVertex source2 = new TestingSourceLineageVertex("source2");
        SourceLineageVertex source3 = new TestingSourceLineageVertex("source3");
        LineageVertex sink1 = new TestingLineageVertex("sink1");
        LineageVertex sink2 = new TestingLineageVertex("sink2");
        LineageGraph lineageGraph =
                DefaultLineageGraph.builder()
                        .addLineageEdge(new TestingLineageEdge(source1, sink1))
                        .addLineageEdges(
                                new TestingLineageEdge(source2, sink2),
                                new TestingLineageEdge(source3, sink1),
                                new TestingLineageEdge(source1, sink2))
                        .build();
        assertThat(lineageGraph.sources()).containsExactlyInAnyOrder(source1, source2, source3);
        assertThat(lineageGraph.sinks()).containsExactlyInAnyOrder(sink1, sink2);
        assertThat(lineageGraph.relations()).hasSize(4);
    }

    /** Testing sink lineage vertex. */
    static class TestingLineageVertex implements LineageVertex {
        private final String id;

        private TestingLineageVertex(String id) {
            this.id = id;
        }

        private String id() {
            return id;
        }

        @Override
        public List<LineageDataset> datasets() {
            return new ArrayList<>();
        }
    }

    /** Testing source lineage vertex. */
    static class TestingSourceLineageVertex extends TestingLineageVertex
            implements SourceLineageVertex {

        private TestingSourceLineageVertex(String id) {
            super(id);
        }

        @Override
        public Boundedness boundedness() {
            return Boundedness.BOUNDED;
        }
    }

    /** Testing lineage edge. */
    static class TestingLineageEdge implements LineageEdge {
        private final SourceLineageVertex source;
        private final LineageVertex sink;

        private TestingLineageEdge(SourceLineageVertex source, LineageVertex sink) {
            this.source = source;
            this.sink = sink;
        }

        @Override
        public SourceLineageVertex source() {
            return source;
        }

        @Override
        public LineageVertex sink() {
            return sink;
        }
    }
}
