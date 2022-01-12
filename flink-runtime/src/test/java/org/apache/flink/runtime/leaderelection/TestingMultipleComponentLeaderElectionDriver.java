/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Optional;

/** Testing implementation of {@link MultipleComponentLeaderElectionDriver}. */
public class TestingMultipleComponentLeaderElectionDriver
        implements MultipleComponentLeaderElectionDriver {

    private final BiConsumerWithException<String, LeaderInformation, Exception>
            publishLeaderInformationConsumer;
    private final ThrowingConsumer<String, Exception> deleteLeaderInformationConsumer;
    private boolean hasLeadership;

    private Optional<Listener> listener;

    private TestingMultipleComponentLeaderElectionDriver(
            BiConsumerWithException<String, LeaderInformation, Exception>
                    publishLeaderInformationConsumer,
            ThrowingConsumer<String, Exception> deleteLeaderInformationConsumer) {
        this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
        this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
        hasLeadership = false;
        listener = Optional.empty();
    }

    public void grantLeadership() {
        if (!hasLeadership) {
            hasLeadership = true;
            listener.ifPresent(Listener::isLeader);
        }
    }

    public void revokeLeadership() {
        if (hasLeadership) {
            hasLeadership = false;
            listener.ifPresent(Listener::notLeader);
        }
    }

    public void setListener(Listener listener) {
        Preconditions.checkState(!this.listener.isPresent(), "Can only set a single listener.");
        this.listener = Optional.of(listener);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasLeadership() {
        return hasLeadership;
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
            throws Exception {
        publishLeaderInformationConsumer.accept(componentId, leaderInformation);
    }

    @Override
    public void deleteLeaderInformation(String componentId) throws Exception {
        deleteLeaderInformationConsumer.accept(componentId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private BiConsumerWithException<String, LeaderInformation, Exception>
                publishLeaderInformationConsumer = (ignoredA, ignoredB) -> {};
        private ThrowingConsumer<String, Exception> deleteLeaderInformationConsumer = ignored -> {};

        public Builder setPublishLeaderInformationConsumer(
                BiConsumerWithException<String, LeaderInformation, Exception>
                        publishLeaderInformationConsumer) {
            this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
            return this;
        }

        public Builder setDeleteLeaderInformationConsumer(
                ThrowingConsumer<String, Exception> deleteLeaderInformationConsumer) {
            this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
            return this;
        }

        public TestingMultipleComponentLeaderElectionDriver build() {
            return new TestingMultipleComponentLeaderElectionDriver(
                    publishLeaderInformationConsumer, deleteLeaderInformationConsumer);
        }
    }
}
