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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.TriConsumer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * {@code TestingLeaderElectionDriver} is a generic test implementation of {@link
 * LeaderElectionDriver} which can be used in test cases.
 */
public class TestingLeaderElectionDriver implements LeaderElectionDriver {

    private final Function<ReentrantLock, Boolean> hasLeadershipFunction;
    private final TriConsumer<ReentrantLock, String, LeaderInformation>
            publishLeaderInformationConsumer;
    private final BiConsumer<ReentrantLock, String> deleteLeaderInformationConsumer;

    private final ThrowingConsumer<ReentrantLock, Exception> closeConsumer;

    private final ReentrantLock lock = new ReentrantLock();

    public TestingLeaderElectionDriver(
            Function<ReentrantLock, Boolean> hasLeadershipFunction,
            TriConsumer<ReentrantLock, String, LeaderInformation> publishLeaderInformationConsumer,
            BiConsumer<ReentrantLock, String> deleteLeaderInformationConsumer,
            ThrowingConsumer<ReentrantLock, Exception> closeConsumer) {
        this.hasLeadershipFunction = hasLeadershipFunction;
        this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
        this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
        this.closeConsumer = closeConsumer;
    }

    @Override
    public boolean hasLeadership() {
        return hasLeadershipFunction.apply(lock);
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        publishLeaderInformationConsumer.accept(lock, componentId, leaderInformation);
    }

    @Override
    public void deleteLeaderInformation(String componentId) {
        deleteLeaderInformationConsumer.accept(lock, componentId);
    }

    @Override
    public void close() throws Exception {
        closeConsumer.accept(lock);
    }

    public static Builder newNoOpBuilder() {
        return new Builder();
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public static Builder newBuilder(AtomicBoolean grantLeadership) {
        return newBuilder(grantLeadership, new AtomicReference<>(), new AtomicBoolean());
    }

    /**
     * Returns a {@code Builder} that comes with a basic default implementation of the {@link
     * LeaderElectionDriver} contract using the passed parameters for information storage.
     *
     * @param hasLeadership saves the current leadership state of the instance that is created from
     *     the {@code Builder}.
     * @param storedLeaderInformation saves the leader information that would be otherwise stored in
     *     some external storage.
     * @param isClosed saves the running state of the driver.
     */
    public static Builder newBuilder(
            AtomicBoolean hasLeadership,
            AtomicReference<LeaderInformationRegister> storedLeaderInformation,
            AtomicBoolean isClosed) {
        Preconditions.checkState(
                storedLeaderInformation.get() == null
                        || !storedLeaderInformation
                                .get()
                                .getRegisteredComponentIds()
                                .iterator()
                                .hasNext(),
                "Initial state check for storedLeaderInformation failed.");
        Preconditions.checkState(!isClosed.get(), "Initial state check for isClosed failed.");
        return newNoOpBuilder()
                .setHasLeadershipFunction(
                        lock -> {
                            try {
                                lock.lock();
                                return hasLeadership.get();
                            } finally {
                                lock.unlock();
                            }
                        })
                .setPublishLeaderInformationConsumer(
                        (lock, componentId, leaderInformation) -> {
                            try {
                                lock.lock();
                                if (hasLeadership.get()) {
                                    storedLeaderInformation.getAndUpdate(
                                            oldData ->
                                                    LeaderInformationRegister.merge(
                                                            oldData,
                                                            componentId,
                                                            leaderInformation));
                                }
                            } finally {
                                lock.unlock();
                            }
                        })
                .setDeleteLeaderInformationConsumer(
                        (lock, componentId) -> {
                            try {
                                lock.lock();
                                if (hasLeadership.get()) {
                                    storedLeaderInformation.getAndUpdate(
                                            oldData ->
                                                    LeaderInformationRegister.clear(
                                                            oldData, componentId));
                                }
                            } finally {
                                lock.unlock();
                            }
                        })
                .setCloseConsumer(
                        lock -> {
                            try {
                                lock.lock();
                                isClosed.set(true);
                            } finally {
                                lock.unlock();
                            }
                        });
    }

    /**
     * {@code Factory} implements {@link LeaderElectionDriverFactory} for the {@code
     * TestingLeaderElectionDriver}.
     */
    public static class Factory implements LeaderElectionDriverFactory {

        private final Builder driverBuilder;

        private final Queue<TestingLeaderElectionDriver> createdDrivers =
                new ConcurrentLinkedQueue<>();

        public static Factory createFactoryWithNoOpDriver() {
            return new Factory(TestingLeaderElectionDriver.newNoOpBuilder());
        }

        public static Factory defaultDriverFactory(
                AtomicBoolean hasLeadership,
                AtomicReference<LeaderInformationRegister> storedLeaderInformation,
                AtomicBoolean isClosed) {
            return new Factory(
                    TestingLeaderElectionDriver.newBuilder(
                            hasLeadership, storedLeaderInformation, isClosed));
        }

        public Factory(Builder driverBuilder) {
            this.driverBuilder = driverBuilder;
        }

        @Override
        public LeaderElectionDriver create(Listener leaderElectionListener) throws Exception {
            final TestingLeaderElectionDriver driver = driverBuilder.build(leaderElectionListener);
            createdDrivers.add(driver);

            return driver;
        }

        /**
         * Returns the {@link TestingLeaderElectionDriver} instance that was created by this {@code
         * Factory} and verifies that no other driver was created.
         *
         * @return The only {@code LeaderElectionDriver} that was created by this {@code Factory}.
         * @throws AssertionError if no {@code LeaderElectionDriver} or more than one instance was
         *     created by this {@code Factory}.
         */
        public TestingLeaderElectionDriver assertAndGetOnlyCreatedDriver() {
            final TestingLeaderElectionDriver driver = createdDrivers.poll();
            if (driver == null) {
                throw new AssertionError("No driver was created by this factory, yet.");
            } else if (!createdDrivers.isEmpty()) {
                throw new AssertionError("More than one driver was created by this factory.");
            }

            return driver;
        }

        public int getCreatedDriverCount() {
            return createdDrivers.size();
        }
    }

    /** {@link Builder} for creating {@link TestingLeaderElectionDriver} instances. */
    public static class Builder {

        private Function<ReentrantLock, Boolean> hasLeadershipFunction = ignoredLock -> false;
        private TriConsumer<ReentrantLock, String, LeaderInformation>
                publishLeaderInformationConsumer =
                        (ignoredLock, ignoredComponentId, ignoredLeaderInformation) -> {};
        private BiConsumer<ReentrantLock, String> deleteLeaderInformationConsumer =
                (ignoredLock, ignoredComponentId) -> {};

        private ThrowingConsumer<ReentrantLock, Exception> closeConsumer = (ignoredLock) -> {};

        private Builder() {}

        public Builder setHasLeadershipFunction(
                Function<ReentrantLock, Boolean> hasLeadershipFunction) {
            this.hasLeadershipFunction = hasLeadershipFunction;
            return this;
        }

        public Builder setPublishLeaderInformationConsumer(
                TriConsumer<ReentrantLock, String, LeaderInformation>
                        publishLeaderInformationConsumer) {
            this.publishLeaderInformationConsumer = publishLeaderInformationConsumer;
            return this;
        }

        public Builder setDeleteLeaderInformationConsumer(
                BiConsumer<ReentrantLock, String> deleteLeaderInformationConsumer) {
            this.deleteLeaderInformationConsumer = deleteLeaderInformationConsumer;
            return this;
        }

        public Builder setCloseConsumer(ThrowingConsumer<ReentrantLock, Exception> closeConsumer) {
            this.closeConsumer = closeConsumer;
            return this;
        }

        public TestingLeaderElectionDriver build(Listener ignoredListener) {
            return new TestingLeaderElectionDriver(
                    hasLeadershipFunction,
                    publishLeaderInformationConsumer,
                    deleteLeaderInformationConsumer,
                    closeConsumer);
        }
    }
}
