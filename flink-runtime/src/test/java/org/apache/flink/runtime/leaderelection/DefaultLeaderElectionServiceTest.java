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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultLeaderElectionService}. */
class DefaultLeaderElectionServiceTest {

    private static final String TEST_URL = "akka//user/jobmanager";

    @Test
    void testOnGrantAndRevokeLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            // grant leadership
                            testingLeaderElectionDriver.isLeader();

                            testingContender.waitForLeader();
                            assertThat(testingContender.getDescription()).isEqualTo(TEST_URL);
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(leaderElectionService.getLeaderSessionID());
                            // Check the external storage
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(
                                            LeaderInformation.known(
                                                    leaderElectionService.getLeaderSessionID(),
                                                    TEST_URL));

                            // revoke leadership
                            testingLeaderElectionDriver.notLeader();
                            testingContender.waitForRevokeLeader();
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                            assertThat(leaderElectionService.getLeaderSessionID()).isNull();
                            // External storage should be cleared
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangedAndShouldBeCorrected() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();

                            final LeaderInformation expectedLeader =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(), TEST_URL);

                            // Leader information changed on external storage. It should be
                            // corrected.
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);

                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address"));
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);
                        });
            }
        };
    }

    @Test
    void testHasLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isNotNull();
                            assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId))
                                    .isTrue();
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isFalse();

                            leaderElectionService.stop();
                            assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId))
                                    .isFalse();
                        });
            }
        };
    }

    @Test
    void testLeaderInformationChangedIfNotBeingLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation faultyLeader =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            testingLeaderElectionDriver.leaderInformationChanged(faultyLeader);
                            // External storage should keep the wrong value.
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(faultyLeader);
                        });
            }
        };
    }

    @Test
    void testOnGrantLeadershipIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.isLeader();
                            // leader contender is not granted leadership
                            assertThat(testingContender.getLeaderSessionID()).isNull();
                        });
            }
        };
    }

    @Test
    void testOnLeaderInformationChangeIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();

                            leaderElectionService.stop();
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());

                            // External storage should not be corrected
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testOnRevokeLeadershipIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID oldSessionId = leaderElectionService.getLeaderSessionID();
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);

                            leaderElectionService.stop();

                            testingLeaderElectionDriver.notLeader();
                            // leader contender is not revoked leadership
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);
                        });
            }
        };
    }

    @Test
    void testOldConfirmLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isNotNull();

                            // Old confirm call should be ignored.
                            leaderElectionService.confirmLeadership(UUID.randomUUID(), TEST_URL);
                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .isEqualTo(currentLeaderSessionId);
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
                            final Exception testException = new Exception("test leader exception");

                            testingLeaderElectionDriver.onFatalError(testException);

                            testingContender.waitForError();
                            assertThat(testingContender.getError())
                                    .isNotNull()
                                    .hasCause(testException);
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
                            final Exception testException = new Exception("test leader exception");

                            leaderElectionService.stop();
                            testingLeaderElectionDriver.onFatalError(testException);
                            assertThat(testingContender.getError()).isNull();
                        });
            }
        };
    }

    /**
     * Tests that we can shut down the DefaultLeaderElectionService if the used LeaderElectionDriver
     * holds an internal lock. See FLINK-20008 for more details.
     */
    @Test
    void testServiceShutDownWithSynchronizedDriver() throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);

        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);

        leaderElectionService.start(testingContender);
        final TestingLeaderElectionDriver currentLeaderDriver =
                Preconditions.checkNotNull(
                        testingLeaderElectionDriverFactory.getCurrentLeaderDriver());

        final CheckedThread isLeaderThread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        currentLeaderDriver.isLeader();
                    }
                };
        isLeaderThread.start();

        leaderElectionService.stop();
        isLeaderThread.sync();
    }

    private static class Context {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);
        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);

        TestingLeaderElectionDriver testingLeaderElectionDriver;

        void runTest(RunnableWithException testMethod) throws Exception {
            leaderElectionService.start(testingContender);

            testingLeaderElectionDriver =
                    testingLeaderElectionDriverFactory.getCurrentLeaderDriver();
            assertThat(testingLeaderElectionDriver).isNotNull();
            testMethod.run();

            leaderElectionService.stop();
        }
    }
}
