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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Testing for lineage graph util. */
class LineageGraphUtilsTest {
    private static final String SOURCE_DATASET_NAME = "LineageSource";
    private static final String SOURCE_DATASET_NAMESPACE = "source://LineageSource";
    private static final String SINK_DATASET_NAME = "LineageSink";
    private static final String SINK_DATASET_NAMESPACE = "sink://LineageSink";

    private static final String LEGACY_SOURCE_DATASET_NAME = "LineageSourceFunction";
    private static final String LEGACY_SOURCE_DATASET_NAMESPACE = "source://LineageSourceFunction";
    private static final String LEGACY_SINK_DATASET_NAME = "LineageSinkFunction";
    private static final String LEGACY_SINK_DATASET_NAMESPACE = "sink://LineageSinkFunction";

    @Test
    void testExtractLineageGraphFromLegacyTransformations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new LineageSourceFunction());
        DataStreamSink<Long> sink = source.addSink(new LineageSinkFunction());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness())
                .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(LEGACY_SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(LEGACY_SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(LEGACY_SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(LEGACY_SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.relations().size()).isEqualTo(1);
    }

    @Test
    void testExtractLineageGraphFromTransformations() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.relations().size()).isEqualTo(1);
    }

    private static class LineageSink extends DiscardingSink<Long> implements LineageVertexProvider {
        public LineageSink() {
            super();
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SINK_DATASET_NAME, SINK_DATASET_NAMESPACE, new HashMap<>());
            DefaultLineageVertex lineageVertex = new DefaultLineageVertex();
            lineageVertex.addLineageDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSource extends NumberSequenceSource
            implements LineageVertexProvider {

        public LineageSource(long from, long to) {
            super(from, to);
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE, new HashMap<>());
            DefaultSourceLineageVertex lineageVertex =
                    new DefaultSourceLineageVertex(Boundedness.BOUNDED);
            lineageVertex.addDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSourceFunction
            implements SourceFunction<Long>, LineageVertexProvider {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long next = 0;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            LEGACY_SOURCE_DATASET_NAME,
                            LEGACY_SOURCE_DATASET_NAMESPACE,
                            new HashMap<>());
            DefaultSourceLineageVertex lineageVertex =
                    new DefaultSourceLineageVertex(Boundedness.CONTINUOUS_UNBOUNDED);
            lineageVertex.addDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSinkFunction implements SinkFunction<Long>, LineageVertexProvider {

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            LEGACY_SINK_DATASET_NAME,
                            LEGACY_SINK_DATASET_NAMESPACE,
                            new HashMap<>());
            DefaultLineageVertex lineageVertex = new DefaultLineageVertex();
            lineageVertex.addLineageDataset(lineageDataset);
            return lineageVertex;
        }
    }
}
