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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

/**
 * Internal implementation to commit a specific committable and handle the response.
 *
 * @param <CommT> type of committable
 */
@Internal
public class CommitRequestImpl<CommT> implements Committer.CommitRequest<CommT> {

    private CommT committable;
    private int numRetries;
    private CommitRequestState state;
    private SinkCommitterMetricGroup metricGroup;

    protected CommitRequestImpl(CommT committable, SinkCommitterMetricGroup metricGroup) {
        this.committable = committable;
        this.metricGroup = metricGroup;
        state = CommitRequestState.RECEIVED;
    }

    protected CommitRequestImpl(
            CommT committable,
            int numRetries,
            CommitRequestState state,
            SinkCommitterMetricGroup metricGroup) {
        this.committable = committable;
        this.numRetries = numRetries;
        this.state = state;
        this.metricGroup = metricGroup;
    }

    boolean isFinished() {
        return state.isFinalState();
    }

    CommitRequestState getState() {
        return state;
    }

    @Override
    public CommT getCommittable() {
        return committable;
    }

    @Override
    public int getNumberOfRetries() {
        return numRetries;
    }

    @Override
    public void signalFailedWithKnownReason(Throwable t) {
        state = CommitRequestState.FAILED;
        metricGroup.getNumCommittablesFailureCounter().inc();
        // let the user configure a strategy for failing and apply it here
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
        state = CommitRequestState.FAILED;
        metricGroup.getNumCommittablesFailureCounter().inc();
        // let the user configure a strategy for failing and apply it here
        throw new IllegalStateException("Failed to commit " + committable, t);
    }

    @Override
    public void retryLater() {
        state = CommitRequestState.RETRY;
        numRetries++;
        metricGroup.getNumCommittablesRetryCounter().inc();
    }

    @Override
    public void updateAndRetryLater(CommT committable) {
        this.committable = committable;
        retryLater();
    }

    @Override
    public void signalAlreadyCommitted() {
        state = CommitRequestState.COMMITTED;
        metricGroup.getNumCommittablesAlreadyCommittedCounter().inc();
    }

    void setSelected() {
        state = CommitRequestState.RECEIVED;
    }

    void setCommittedIfNoError() {
        if (state == CommitRequestState.RECEIVED) {
            state = CommitRequestState.COMMITTED;
            metricGroup.getNumCommittablesSuccessCounter().inc();
        }
    }

    CommitRequestImpl<CommT> copy() {
        return new CommitRequestImpl<>(committable, numRetries, state, metricGroup);
    }
}
