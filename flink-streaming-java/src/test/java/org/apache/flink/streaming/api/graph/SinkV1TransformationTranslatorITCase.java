/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link org.apache.flink.streaming.api.transformations.SinkTransformation}.
 *
 * <p>ATTENTION: This test is extremely brittle. Do NOT remove, add or re-order test cases.
 */
@ExtendWith(ParameterizedTestExtension.class)
class SinkV1TransformationTranslatorITCase
        extends SinkTransformationTranslatorITCaseBase<Sink<Integer, ?, ?, ?>> {

    @Override
    Sink<Integer, ?, ?, ?> simpleSink() {
        return TestSink.newBuilder().build();
    }

    @Override
    Sink<Integer, ?, ?, ?> sinkWithCommitter() {
        return TestSink.newBuilder().setDefaultCommitter().build();
    }

    @Override
    Sink<Integer, ?, ?, ?> sinkWithGlobalCommitter() {
        return TestSink.newBuilder().setDefaultCommitter().setDefaultGlobalCommitter().build();
    }

    @Override
    DataStreamSink<Integer> sinkTo(DataStream<Integer> stream, Sink<Integer, ?, ?, ?> sink) {
        return stream.sinkTo(sink);
    }

    @Override
    DataStreamSink<Integer> sinkTo(
            DataStream<Integer> stream,
            Sink<Integer, ?, ?, ?> sink,
            CustomSinkOperatorUidHashes hashes) {
        return stream.sinkTo(sink, hashes);
    }

    @TestTemplate
    void generateWriterCommitterGlobalCommitterTopology() {

        final StreamGraph streamGraph =
                buildGraph(
                        TestSink.newBuilder()
                                .setDefaultCommitter()
                                .setDefaultGlobalCommitter()
                                .build(),
                        runtimeExecutionMode);

        final StreamNode sourceNode = findNodeName(streamGraph, node -> node.contains("Source"));
        final StreamNode writerNode = findWriter(streamGraph);
        final StreamNode committerNode = findCommitter(streamGraph);

        validateTopology(
                sourceNode,
                IntSerializer.class,
                writerNode,
                SinkWriterOperatorFactory.class,
                PARALLELISM,
                -1);
        assertNoUnalignedOutput(writerNode);

        if (runtimeExecutionMode == RuntimeExecutionMode.STREAMING) {
            // in streaming writer and committer are merged into one operator

            assertThat(streamGraph.getStreamNodes()).hasSize(4);
        } else {
            assertThat(streamGraph.getStreamNodes()).hasSize(4);
            validateTopology(
                    writerNode,
                    SimpleVersionedSerializerTypeSerializerProxy.class,
                    committerNode,
                    CommitterOperatorFactory.class,
                    PARALLELISM,
                    -1);
            assertNoUnalignedOutput(committerNode);
        }

        final StreamNode globalCommitterNode = findGlobalCommitter(streamGraph);
        validateTopology(
                committerNode,
                SimpleVersionedSerializerTypeSerializerProxy.class,
                globalCommitterNode,
                SimpleOperatorFactory.class,
                1,
                1);
    }

    /**
     * It is not possible anymore with Sink V2 to have a topology consisting only of Sink Writer and
     * a Global Committer. The SinkV1Adapter translates these topologies into a Sink Writer, an only
     * forwarding committer and the Global Committer.
     */
    @TestTemplate
    void generateWriterGlobalCommitterTopology() {
        final StreamGraph streamGraph =
                buildGraph(
                        TestSink.newBuilder().setDefaultGlobalCommitter().build(),
                        runtimeExecutionMode);

        final StreamNode sourceNode = findNodeName(streamGraph, node -> node.contains("Source"));
        final StreamNode writerNode = findWriter(streamGraph);

        validateTopology(
                sourceNode,
                IntSerializer.class,
                writerNode,
                SinkWriterOperatorFactory.class,
                PARALLELISM,
                -1);

        final StreamNode committerNode = findCommitter(streamGraph);
        final StreamNode globalCommitterNode = findGlobalCommitter(streamGraph);

        assertNoUnalignedOutput(writerNode);
        validateTopology(
                writerNode,
                SimpleVersionedSerializerTypeSerializerProxy.class,
                committerNode,
                CommitterOperatorFactory.class,
                PARALLELISM,
                -1);

        assertNoUnalignedOutput(committerNode);
        validateTopology(
                committerNode,
                SimpleVersionedSerializerTypeSerializerProxy.class,
                globalCommitterNode,
                SimpleOperatorFactory.class,
                1,
                1);
    }
}
