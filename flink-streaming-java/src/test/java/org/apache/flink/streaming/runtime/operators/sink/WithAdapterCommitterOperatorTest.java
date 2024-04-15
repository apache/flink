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

import java.util.Collections;
import java.util.List;

class WithAdapterCommitterOperatorTest extends CommitterOperatorTestBase {

    @Override
    SinkAndCounters sinkWithPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSink.newBuilder()
                                .setCommitter(committer)
                                .setDefaultGlobalCommitter()
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build()
                                .asV2(),
                () -> committer.successfulCommits);
    }

    @Override
    SinkAndCounters sinkWithPostCommitWithRetry() {
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSink.newBuilder()
                                .setCommitter(new TestSink.RetryOnceCommitter())
                                .setDefaultGlobalCommitter()
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build()
                                .asV2(),
                () -> 0);
    }

    @Override
    SinkAndCounters sinkWithoutPostCommit() {
        ForwardingCommitter committer = new ForwardingCommitter();
        return new SinkAndCounters(
                (SupportsCommitter<String>)
                        TestSink.newBuilder()
                                .setCommitter(committer)
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build()
                                .asV2(),
                () -> committer.successfulCommits);
    }

    private static class ForwardingCommitter extends TestSink.DefaultCommitter {
        private int successfulCommits = 0;

        @Override
        public List<String> commit(List<String> committables) {
            successfulCommits += committables.size();
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {}
    }
}
