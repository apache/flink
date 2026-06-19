/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.sink2.mocks;

import org.apache.flink.api.connector.sink2.Committer;

/**
 * A simple {@link Committer.CommitRequest} used for testing.
 *
 * @param <CommT> committable type
 */
public class MockCommitRequest<CommT> implements Committer.CommitRequest<CommT> {

    private final CommT committable;
    private int retries = 0;
    private Throwable failedWithUnknownReason;

    public MockCommitRequest(CommT committable) {
        this.committable = committable;
    }

    @Override
    public CommT getCommittable() {
        return committable;
    }

    @Override
    public int getNumberOfRetries() {
        return retries;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {}

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
        failedWithUnknownReason = t;
    }

    @Override
    public void retryLater() {
        retries++;
    }

    @Override
    public void updateAndRetryLater(CommT committable) {}

    @Override
    public void signalAlreadyCommitted() {}

    public Throwable getFailedWithUnknownReason() {
        return failedWithUnknownReason;
    }
}
