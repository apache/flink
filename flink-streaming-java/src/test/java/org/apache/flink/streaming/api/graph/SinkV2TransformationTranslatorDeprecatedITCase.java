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

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.sink.deprecated.TestSinkV2;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link org.apache.flink.streaming.api.transformations.SinkTransformation}.
 *
 * <p>ATTENTION: This test is extremely brittle. Do NOT remove, add or re-order test cases.
 *
 * <p>Should be removed along with {@link
 * org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink}.
 */
@Deprecated
@ExtendWith(ParameterizedTestExtension.class)
class SinkV2TransformationTranslatorDeprecatedITCase
        extends SinkTransformationTranslatorITCaseBase<Sink<Integer>> {

    @Override
    Sink<Integer> simpleSink() {
        return TestSinkV2.<Integer>newBuilder().build();
    }

    @Override
    Sink<Integer> sinkWithCommitter() {
        return TestSinkV2.<Integer>newBuilder().setDefaultCommitter().build();
    }

    @Override
    DataStreamSink<Integer> sinkTo(DataStream<Integer> stream, Sink<Integer> sink) {
        return stream.sinkTo(sink);
    }

    @TestTemplate
    void testSettingOperatorUidHash() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        final String writerHash = "f6b178ce445dc3ffaa06bad27a51fead";
        final String committerHash = "68ac8ae79eae4e3135a54f9689c4aa10";
        final CustomSinkOperatorUidHashes operatorsUidHashes =
                CustomSinkOperatorUidHashes.builder()
                        .setWriterUidHash(writerHash)
                        .setCommitterUidHash(committerHash)
                        .build();
        src.sinkTo(sinkWithCommitter(), operatorsUidHashes).name(NAME);

        final StreamGraph streamGraph = env.getStreamGraph();

        assertThat(findWriter(streamGraph).getUserHash()).isEqualTo(writerHash);
        assertThat(findCommitter(streamGraph).getUserHash()).isEqualTo(committerHash);
    }

    /**
     * When ever you need to change something in this test case please think about possible state
     * upgrade problems introduced by your changes.
     */
    @TestTemplate
    void testSettingOperatorUids() {
        final String sinkUid = "f6b178ce445dc3ffaa06bad27a51fead";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        src.sinkTo(sinkWithCommitter()).name(NAME).uid(sinkUid);

        final StreamGraph streamGraph = env.getStreamGraph();
        assertThat(findWriter(streamGraph).getTransformationUID()).isEqualTo(sinkUid);
        assertThat(findCommitter(streamGraph).getTransformationUID())
                .isEqualTo(String.format("Sink Committer: %s", sinkUid));
    }
}
