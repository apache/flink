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

import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Testing implementation of {@link MultipleComponentLeaderElectionDriver}. */
public class TestingMultipleComponentLeaderElectionDriver
        implements MultipleComponentLeaderElectionDriver {

    private final BiConsumer<String, LeaderInformation> publishLeaderInformationConsumer;
    private final Consumer<String> deleteLeaderInformationConsumer;
    private boolean hasLeadership;

    private Optional<Listener> listener;
    private Optional<FatalErrorHandler> optionalFatalErrorHandler;

    private TestingMultipleComponentLeaderElectionDriver(
            BiConsumer<String, LeaderInformation> publishLeaderInformationConsumer,
            Consumer<String> deleteLeaderInformationConsumer) {
        this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
        this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
        hasLeadership = false;
        listener = Optional.empty();
        optionalFatalErrorHandler = Optional.empty();
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

    public void triggerExternalChangeOfLeaderInformation(
            String contenderID, LeaderInformation leaderInformation) {
        listener.ifPresent(
                listener -> listener.notifyLeaderInformationChange(contenderID, leaderInformation));
    }

    public void triggerExternalFatalError(Throwable fatalError) {
        this.optionalFatalErrorHandler.ifPresent(
                fatalErrorHandler -> fatalErrorHandler.onFatalError(fatalError));
    }

    public void setListener(Listener listener) {
        Preconditions.checkState(!this.listener.isPresent(), "Can only set a single listener.");
        this.listener = Optional.of(listener);
    }

    public void setFatalErrorHandler(@Nullable FatalErrorHandler fatalErrorHandler) {
        Preconditions.checkState(
                !this.optionalFatalErrorHandler.isPresent(),
                "Only a single fatalErrorHandler can be specified.");
        this.optionalFatalErrorHandler = Optional.ofNullable(fatalErrorHandler);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasLeadership() {
        return hasLeadership;
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        publishLeaderInformationConsumer.accept(componentId, leaderInformation);
    }

    @Override
    public void deleteLeaderInformation(String componentId) {
        deleteLeaderInformationConsumer.accept(componentId);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private BiConsumer<String, LeaderInformation> publishLeaderInformationConsumer =
                (ignoredA, ignoredB) -> {};
        private Consumer<String> deleteLeaderInformationConsumer = ignored -> {};

        public Builder setPublishLeaderInformationConsumer(
                BiConsumer<String, LeaderInformation> publishLeaderInformationConsumer) {
            this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
            return this;
        }

        public Builder setDeleteLeaderInformationConsumer(
                Consumer<String> deleteLeaderInformationConsumer) {
            this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
            return this;
        }

        public TestingMultipleComponentLeaderElectionDriver build() {
            return new TestingMultipleComponentLeaderElectionDriver(
                    publishLeaderInformationConsumer, deleteLeaderInformationConsumer);
        }
    }
}
