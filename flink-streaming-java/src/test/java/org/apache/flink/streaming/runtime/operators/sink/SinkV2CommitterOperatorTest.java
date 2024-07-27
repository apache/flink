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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink2.SupportsCommitter;

import java.util.Collection;

class SinkV2CommitterOperatorTest extends CommitterOperatorTestBase {
    @Override
    SinkAndCounters sinkWithPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSinkV2.newBuilder()
                                .setCommitter(committer)
                                .setCommittableSerializer(TestSinkV2.StringSerializer.INSTANCE)
                                .setWithPostCommitTopology(true)
                                .build(),
                () -> committer.successfulCommits);
    }

    @Override
    SinkAndCounters sinkWithPostCommitWithRetry() {
        return new CommitterOperatorTestBase.SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSinkV2.newBuilder()
                                .setCommitter(new TestSinkV2.RetryOnceCommitter())
                                .setCommittableSerializer(TestSinkV2.StringSerializer.INSTANCE)
                                .setWithPostCommitTopology(true)
                                .build(),
                () -> 0);
    }

    @Override
    SinkAndCounters sinkWithoutPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSinkV2.newBuilder()
                                .setCommitter(committer)
                                .setCommittableSerializer(TestSinkV2.StringSerializer.INSTANCE)
                                .setWithPostCommitTopology(false)
                                .build(),
                () -> committer.successfulCommits);
    }

    private static class ForwardingCommitter extends TestSinkV2.DefaultCommitter {
        private int successfulCommits = 0;

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            successfulCommits += committables.size();
        }

        @Override
        public void close() throws Exception {}
    }
}
