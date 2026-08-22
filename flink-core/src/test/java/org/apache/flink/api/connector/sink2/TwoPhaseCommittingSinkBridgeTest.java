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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class TwoPhaseCommittingSinkBridgeTest {

    @Test
    void twoPhaseCommittingSinkIsRecognizedAsSink() {
        TestTwoPhaseCommittingSink sink = new TestTwoPhaseCommittingSink();
        assertThat(sink).isInstanceOf(Sink.class);
    }

    @Test
    void twoPhaseCommittingSinkIsRecognizedAsSupportsCommitter() {
        TestTwoPhaseCommittingSink sink = new TestTwoPhaseCommittingSink();
        assertThat(sink).isInstanceOf(SupportsCommitter.class);
    }

    @Test
    void precommittingSinkWriterExtendsCommittingSinkWriter() {
        assertThat(CommittingSinkWriter.class)
                .isAssignableFrom(TwoPhaseCommittingSink.PrecommittingSinkWriter.class);
    }

    @Test
    void noArgCreateCommitterDelegation() throws IOException {
        LegacyTwoPhaseCommittingSink sink = new LegacyTwoPhaseCommittingSink();
        Committer<String> committer = sink.createCommitter(null);
        assertThat(committer).isNotNull();
    }

    private static class TestTwoPhaseCommittingSink
            implements TwoPhaseCommittingSink<String, String> {

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new TestPrecommittingWriter();
        }

        @Override
        public Committer<String> createCommitter(CommitterInitContext context) throws IOException {
            return new TestCommitter();
        }

        @Override
        public SimpleVersionedSerializer<String> getCommittableSerializer() {
            return new SimpleVersionedSerializer<>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(String obj) {
                    return new byte[0];
                }

                @Override
                public String deserialize(int version, byte[] serialized) {
                    return "";
                }
            };
        }
    }

    /** Simulates a 1.x connector that only overrides the no-arg createCommitter(). */
    private static class LegacyTwoPhaseCommittingSink
            implements TwoPhaseCommittingSink<String, String> {

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
            return new TestPrecommittingWriter();
        }

        @Override
        public Committer<String> createCommitter() throws IOException {
            return new TestCommitter();
        }

        @Override
        public SimpleVersionedSerializer<String> getCommittableSerializer() {
            return new SimpleVersionedSerializer<>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(String obj) {
                    return new byte[0];
                }

                @Override
                public String deserialize(int version, byte[] serialized) {
                    return "";
                }
            };
        }
    }

    private static class TestPrecommittingWriter
            implements TwoPhaseCommittingSink.PrecommittingSinkWriter<String, String> {

        @Override
        public Collection<String> prepareCommit() {
            return Collections.emptyList();
        }

        @Override
        public void write(String element, Context context) {}

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
    }

    private static class TestCommitter implements Committer<String> {

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {}

        @Override
        public void close() {}
    }
}
