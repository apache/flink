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
import org.apache.flink.configuration.CommitFailureStrategy;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Internal implementation to commit a specific committable and handle the response.
 *
 * @param <CommT> type of committable
 */
@Internal
public class CommitRequestImpl<CommT> implements Committer.CommitRequest<CommT> {

    private static final Logger LOG = LoggerFactory.getLogger(CommitRequestImpl.class);

    private CommT committable;
    private int numRetries;
    private CommitRequestState state;
    private SinkCommitterMetricGroup metricGroup;
    private CommitFailureStrategy failureStrategy = CommitFailureStrategy.FAIL;

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

    void setFailureStrategy(CommitFailureStrategy failureStrategy) {
        this.failureStrategy = failureStrategy;
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
        // Known failures are always discarded; see CommitFailureStrategy for unknown failures.
        LOG.warn("Commit failed for committable [{}] with known reason. Discarding.", committable, t);
    }

    @Override
    public void signalFailedWithUnknownReason(Throwable t) {
        state = CommitRequestState.FAILED;
        metricGroup.getNumCommittablesFailureCounter().inc();
        if (failureStrategy == CommitFailureStrategy.WARN) {
            LOG.warn(
                    "Commit failed for committable [{}] with unknown reason. "
                            + "Skipping due to failure strategy WARN.",
                    committable,
                    t);
        } else {
            throw new IllegalStateException("Failed to commit " + committable, t);
        }
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
        CommitRequestImpl<CommT> copied =
                new CommitRequestImpl<>(committable, numRetries, state, metricGroup);
        copied.failureStrategy = this.failureStrategy;
        return copied;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitRequestImpl<?> that = (CommitRequestImpl<?>) o;
        return numRetries == that.numRetries
                && Objects.equals(committable, that.committable)
                && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(committable, numRetries, state);
    }

    @Override
    public String toString() {
        return "CommitRequestImpl{"
                + "state="
                + state
                + ", numRetries="
                + numRetries
                + ", committable="
                + committable
                + '}';
    }
}
