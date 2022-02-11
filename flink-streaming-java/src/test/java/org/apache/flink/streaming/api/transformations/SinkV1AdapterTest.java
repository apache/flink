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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SinkV1AdapterTest {

    @ParameterizedTest
    @MethodSource("provideSinkCombinations")
    void testSinkCombinations(
            org.apache.flink.api.connector.sink.Sink<?, ?, ?, ?> sinkV1,
            Collection<Class<Sink<?>>> sinkInterfaces) {
        final Sink<?> converted = SinkV1Adapter.wrap(sinkV1);
        for (final Class<Sink<?>> sinkInterface : sinkInterfaces) {
            assertThat(converted).isInstanceOf(sinkInterface);
        }
    }

    private static List<Arguments> provideSinkCombinations() {
        return Arrays.asList(
                Arguments.of(new DefaultSinkV1(), Collections.singletonList(Sink.class)),
                Arguments.of(new StateFulSinkV1(), Arrays.asList(Sink.class, StatefulSink.class)),
                Arguments.of(
                        new CommittingSinkV1(),
                        Arrays.asList(Sink.class, TwoPhaseCommittingSink.class)),
                Arguments.of(
                        new StatefulCommittingSinkV1(),
                        Arrays.asList(
                                Sink.class, StatefulSink.class, TwoPhaseCommittingSink.class)),
                Arguments.of(
                        new GlobalCommittingSinkV1(),
                        Arrays.asList(
                                Sink.class,
                                TwoPhaseCommittingSink.class,
                                WithPostCommitTopology.class)),
                Arguments.of(
                        new StatefulGlobalCommittingSinkV1(),
                        Arrays.asList(
                                Sink.class,
                                StatefulSink.class,
                                TwoPhaseCommittingSink.class,
                                WithPostCommitTopology.class)));
    }

    private static class DefaultSinkV1
            implements org.apache.flink.api.connector.sink.Sink<
                    Integer, Integer, Integer, Integer> {

        @Override
        public SinkWriter<Integer, Integer, Integer> createWriter(
                InitContext context, List<Integer> states) throws IOException {
            return null;
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
            return Optional.empty();
        }

        @Override
        public Optional<Committer<Integer>> createCommitter() throws IOException {
            return Optional.empty();
        }

        @Override
        public Optional<GlobalCommitter<Integer, Integer>> createGlobalCommitter()
                throws IOException {
            return Optional.empty();
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getCommittableSerializer() {
            return Optional.empty();
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getGlobalCommittableSerializer() {
            return Optional.empty();
        }
    }

    private static class StateFulSinkV1 extends DefaultSinkV1 {
        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
            return Optional.of(new NoOpSerializer());
        }
    }

    private static class CommittingSinkV1 extends DefaultSinkV1 {
        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getCommittableSerializer() {
            return Optional.of(new NoOpSerializer());
        }

        @Override
        public Optional<Committer<Integer>> createCommitter() throws IOException {
            return Optional.of(
                    new Committer<Integer>() {
                        @Override
                        public List<Integer> commit(List<Integer> committables) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void close() {
                            throw new UnsupportedOperationException();
                        }
                    });
        }
    }

    private static class StatefulCommittingSinkV1 extends CommittingSinkV1 {
        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
            return Optional.of(new NoOpSerializer());
        }
    }

    private static class GlobalCommittingSinkV1 extends CommittingSinkV1 {
        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getGlobalCommittableSerializer() {
            return Optional.of(new NoOpSerializer());
        }
    }

    private static class StatefulGlobalCommittingSinkV1 extends StatefulCommittingSinkV1 {
        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getGlobalCommittableSerializer() {
            return Optional.of(new NoOpSerializer());
        }
    }

    private static class NoOpSerializer implements SimpleVersionedSerializer<Integer> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Integer obj) throws IOException {
            return new byte[0];
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) throws IOException {
            return null;
        }
    }
}
