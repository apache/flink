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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultLeaderElectionService}. */
class DefaultLeaderRetrievalServiceTest {

    private static final String TEST_URL = "pekko://user/jobmanager";

    @Test
    void testNotifyLeaderAddress() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            testingListener.waitForNewLeader();
                            assertThat(testingListener.getLeaderSessionID())
                                    .isEqualTo(newLeader.getLeaderSessionID());
                            assertThat(testingListener.getAddress())
                                    .isEqualTo(newLeader.getLeaderAddress());
                        });
            }
        };
    }

    @Test
    void testNotifyLeaderAddressEmpty() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            testingListener.waitForNewLeader();

                            testingLeaderRetrievalDriver.onUpdate(LeaderInformation.empty());
                            testingListener.waitForEmptyLeaderInformation();
                            assertThat(testingListener.getLeaderSessionID()).isNull();
                            assertThat(testingListener.getAddress()).isNull();
                        });
            }
        };
    }

    @Test
    void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test exception");

                            testingLeaderRetrievalDriver.onFatalError(testException);

                            testingListener.waitForError();
                            assertThat(testingListener.getError()).hasCause(testException);
                        });
            }
        };
    }

    @Test
    void testErrorIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test exception");

                            leaderRetrievalService.stop();
                            testingLeaderRetrievalDriver.onFatalError(testException);

                            assertThat(testingListener.getError()).isNull();
                        });
            }
        };
    }

    @Test
    void testNotifyLeaderAddressOnlyWhenLeaderTrulyChanged() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            assertThat(testingListener.getLeaderEventQueueSize()).isOne();

                            // Same leader information should not be notified twice.
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            assertThat(testingListener.getLeaderEventQueueSize()).isOne();

                            // Leader truly changed.
                            testingLeaderRetrievalDriver.onUpdate(
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL + 1));
                            assertThat(testingListener.getLeaderEventQueueSize()).isEqualTo(2);
                        });
            }
        };
    }

    private static class Context {
        private final TestingLeaderRetrievalDriver.TestingLeaderRetrievalDriverFactory
                leaderRetrievalDriverFactory =
                        new TestingLeaderRetrievalDriver.TestingLeaderRetrievalDriverFactory();
        final DefaultLeaderRetrievalService leaderRetrievalService =
                new DefaultLeaderRetrievalService(leaderRetrievalDriverFactory);
        final TestingListener testingListener = new TestingListener();

        TestingLeaderRetrievalDriver testingLeaderRetrievalDriver;

        void runTest(RunnableWithException testMethod) throws Exception {
            leaderRetrievalService.start(testingListener);

            testingLeaderRetrievalDriver = leaderRetrievalDriverFactory.getCurrentRetrievalDriver();
            assertThat(testingLeaderRetrievalDriver).isNotNull();
            testMethod.run();

            leaderRetrievalService.stop();
        }
    }
}
