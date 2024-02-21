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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * {@code TestingGenericLeaderContender} is a more generic testing implementation of the {@link
 * LeaderContender} interface.
 */
public class TestingGenericLeaderContender implements LeaderContender {

    private final Object lock = new Object();

    private final Consumer<UUID> grantLeadershipConsumer;
    private final Runnable revokeLeadershipRunnable;
    private final Consumer<Exception> handleErrorConsumer;

    private TestingGenericLeaderContender(
            Consumer<UUID> grantLeadershipConsumer,
            Runnable revokeLeadershipRunnable,
            Consumer<Exception> handleErrorConsumer) {
        this.grantLeadershipConsumer = grantLeadershipConsumer;
        this.revokeLeadershipRunnable = revokeLeadershipRunnable;
        this.handleErrorConsumer = handleErrorConsumer;
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        synchronized (lock) {
            grantLeadershipConsumer.accept(leaderSessionID);
        }
    }

    @Override
    public void revokeLeadership() {
        synchronized (lock) {
            revokeLeadershipRunnable.run();
        }
    }

    @Override
    public void handleError(Exception exception) {
        synchronized (lock) {
            handleErrorConsumer.accept(exception);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingGenericLeaderContender} instances. */
    public static class Builder {
        private Consumer<UUID> grantLeadershipConsumer = ignoredSessionID -> {};
        private Runnable revokeLeadershipRunnable = () -> {};
        private Consumer<Exception> handleErrorConsumer =
                error -> {
                    throw new AssertionError(error);
                };

        private Builder() {}

        public Builder setGrantLeadershipConsumer(Consumer<UUID> grantLeadershipConsumer) {
            this.grantLeadershipConsumer = grantLeadershipConsumer;
            return this;
        }

        public Builder setRevokeLeadershipRunnable(Runnable revokeLeadershipRunnable) {
            this.revokeLeadershipRunnable = revokeLeadershipRunnable;
            return this;
        }

        public Builder setHandleErrorConsumer(Consumer<Exception> handleErrorConsumer) {
            this.handleErrorConsumer = handleErrorConsumer;
            return this;
        }

        public TestingGenericLeaderContender build() {
            return new TestingGenericLeaderContender(
                    grantLeadershipConsumer, revokeLeadershipRunnable, handleErrorConsumer);
        }
    }
}
