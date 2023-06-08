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

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * {@code TestingGenericLeaderElectionDriver} is test implementation of {@link LeaderElectionDriver}
 * to support test cases in the most generic way.
 */
public class TestingGenericLeaderElectionDriver implements LeaderElectionDriver {

    private final Consumer<LeaderInformation> writeLeaderInformationConsumer;
    private final Supplier<Boolean> hasLeadershipSupplier;
    private final ThrowingRunnable<Exception> closeRunnable;

    private TestingGenericLeaderElectionDriver(
            Consumer<LeaderInformation> writeLeaderInformationConsumer,
            Supplier<Boolean> hasLeadershipSupplier,
            ThrowingRunnable<Exception> closeRunnable) {
        this.writeLeaderInformationConsumer = writeLeaderInformationConsumer;
        this.hasLeadershipSupplier = hasLeadershipSupplier;
        this.closeRunnable = closeRunnable;
    }

    @Override
    public void writeLeaderInformation(LeaderInformation leaderInformation) {
        writeLeaderInformationConsumer.accept(leaderInformation);
    }

    @Override
    public boolean hasLeadership() {
        return hasLeadershipSupplier.get();
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingGenericLeaderContender} instances. */
    public static class Builder {
        private Consumer<LeaderInformation> writeLeaderInformationConsumer =
                ignoredLeaderInformation -> {};
        private Supplier<Boolean> hasLeadershipSupplier = () -> false;
        private ThrowingRunnable<Exception> closeRunnable = () -> {};

        private Builder() {}

        public Builder setWriteLeaderInformationConsumer(
                Consumer<LeaderInformation> writeLeaderInformationConsumer) {
            this.writeLeaderInformationConsumer = writeLeaderInformationConsumer;
            return this;
        }

        public Builder setHasLeadershipSupplier(Supplier<Boolean> hasLeadershipSupplier) {
            this.hasLeadershipSupplier = hasLeadershipSupplier;
            return this;
        }

        public Builder setCloseRunnable(ThrowingRunnable<Exception> closeRunnable) {
            this.closeRunnable = closeRunnable;
            return this;
        }

        public TestingGenericLeaderElectionDriver build() {
            return new TestingGenericLeaderElectionDriver(
                    writeLeaderInformationConsumer, hasLeadershipSupplier, closeRunnable);
        }
    }
}
