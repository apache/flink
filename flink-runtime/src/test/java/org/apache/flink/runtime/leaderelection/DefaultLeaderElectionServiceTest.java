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
                            testingLeaderElectionDriver.grantLeadership();
                            testingContender.waitForLeader();

                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(leaderElectionService.getLeaderSessionID());
                            // Check the external storage
                            assertThat(leaderInformation)
                                    .isEqualTo(
                                            LeaderInformation.known(
                                                    leaderElectionService.getLeaderSessionID(),
                                                    TEST_URL));

                            // revoke leadership
                            testingLeaderElectionDriver.revokeLeadership();
                            testingContender.waitForRevokeLeader();

                            assertThat(testingContender.getLeaderSessionID()).isNull();
                            assertThat(leaderElectionService.getLeaderSessionID()).isNull();
                            // External storage should be cleared
                            assertThat(leaderInformation).isEqualTo(LeaderInformation.empty());
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
                            final String contenderID = "ignored-contender-id";
                            testingLeaderElectionDriver.grantLeadership();

                            final LeaderInformation expectedLeader =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(), TEST_URL);

                            // Leader information changed on external storage. It should be
                            // corrected.
                            testingLeaderElectionDriver.triggerExternalChangeOfLeaderInformation(
                                    contenderID, LeaderInformation.empty());
                            assertThat(leaderInformation).isEqualTo(expectedLeader);

                            testingLeaderElectionDriver.triggerExternalChangeOfLeaderInformation(
                                    contenderID,
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address"));
                            assertThat(leaderInformation).isEqualTo(expectedLeader);
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
                            testingLeaderElectionDriver.grantLeadership();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();

                            assertThat(currentLeaderSessionId).isNotNull();
                            assertThat(leaderElection.hasLeadership(currentLeaderSessionId))
                                    .isTrue();
                            assertThat(leaderElection.hasLeadership(UUID.randomUUID())).isFalse();

                            leaderElectionService.close();
                            assertThat(leaderElection.hasLeadership(currentLeaderSessionId))
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
                            final String contenderID = "ignored-contender-id";
                            final LeaderInformation faultyLeader =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            leaderInformation = faultyLeader;
                            testingLeaderElectionDriver.triggerExternalChangeOfLeaderInformation(
                                    contenderID, faultyLeader);
                            assertThat(leaderInformation)
                                    .as("External storage should keep the wrong value.")
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
                            leaderElectionService.close();
                            testingLeaderElectionDriver.grantLeadership();
                            assertThat(testingContender.getLeaderSessionID())
                                    .as("LeaderContender should not have acquired the leadership.")
                                    .isNull();
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
                            testingLeaderElectionDriver.grantLeadership();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as("The contender should have a leader session ID assigned.")
                                    .isNotNull();

                            leaderElectionService.close();
                            testingLeaderElectionDriver.triggerExternalChangeOfLeaderInformation(
                                    "ignored-contender-id", LeaderInformation.empty());

                            // External storage should not be corrected
                            assertThat(leaderInformation)
                                    .as(
                                            "The backend data shouldn't have been overwritten by the now stopped LeaderElectionService.")
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    void testOnRevokeLeadershipIsTriggeredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.grantLeadership();
                            final UUID oldSessionId = leaderElectionService.getLeaderSessionID();
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);

                            leaderElectionService.close();

                            assertThat(testingContender.getLeaderSessionID())
                                    .as(
                                            "LeaderContender should have been revoked as part of the stop call.")
                                    .isNull();
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
                            testingLeaderElectionDriver.grantLeadership();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isNotNull();

                            // Old confirm call should be ignored.
                            leaderElection.confirmLeadership(UUID.randomUUID(), TEST_URL);
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
                            final Exception testException =
                                    new Exception(
                                            "Expected test exception simulating a fatal error being raised by the HA backend.");
                            testingLeaderElectionDriver.triggerExternalFatalError(testException);

                            testingContender.waitForError();
                            assertThat(testingContender.getError())
                                    .isInstanceOf(LeaderElectionException.class)
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
                            final Exception testException =
                                    new Exception(
                                            "Expected test exception simulating a fatal error being raised by the HA backend.");

                            leaderElectionService.close();
                            testingLeaderElectionDriver.triggerExternalFatalError(testException);
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
        final TestingMultipleComponentLeaderElectionDriver driver =
                TestingMultipleComponentLeaderElectionDriver.newBuilder().build();
        final TestingMultipleComponentLeaderElectionDriverFactory driverFactory =
                new TestingMultipleComponentLeaderElectionDriverFactory(driver);
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(driverFactory);

        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);
        final LeaderElectionService.LeaderElection leaderElection =
                testingContender.startLeaderElection();

        final CheckedThread isLeaderThread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        driver.grantLeadership();
                    }
                };
        isLeaderThread.start();

        leaderElection.close();
        leaderElectionService.close();
        isLeaderThread.sync();
    }

    private static class Context {
        TestingMultipleComponentLeaderElectionDriver testingLeaderElectionDriver;
        DefaultLeaderElectionService leaderElectionService;
        TestingContender testingContender;

        LeaderElectionService.LeaderElection leaderElection;

        // only a single contender is supported in DefaultLeaderElectionService so far
        LeaderInformation leaderInformation;

        void runTest(RunnableWithException testMethod) throws Exception {
            testingLeaderElectionDriver =
                    TestingMultipleComponentLeaderElectionDriver.newBuilder()
                            .setPublishLeaderInformationConsumer(
                                    (ignoredContenderID, leaderInformation) ->
                                            this.leaderInformation = leaderInformation)
                            .setDeleteLeaderInformationConsumer(
                                    ignoredContenderID ->
                                            this.leaderInformation = LeaderInformation.empty())
                            .build();
            final MultipleComponentLeaderElectionDriverFactory leaderElectionDriverFactory =
                    new TestingMultipleComponentLeaderElectionDriverFactory(
                            testingLeaderElectionDriver);
            leaderElectionService = new DefaultLeaderElectionService(leaderElectionDriverFactory);

            testingContender = new TestingContender(TEST_URL, leaderElectionService);
            leaderElection = testingContender.startLeaderElection();

            testMethod.run();

            leaderElection.close();
            leaderElectionService.close();
        }
    }
}
