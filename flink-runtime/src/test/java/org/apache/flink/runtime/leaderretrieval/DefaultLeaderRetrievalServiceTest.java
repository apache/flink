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

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.TestingListener;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link DefaultLeaderElectionService}. */
public class DefaultLeaderRetrievalServiceTest extends TestLogger {

    private static final String TEST_URL = "akka//user/jobmanager";
    private static final long timeout = 50L;

    @Test
    public void testNotifyLeaderAddress() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            testingListener.waitForNewLeader(timeout);
                            assertThat(
                                    testingListener.getLeaderSessionID(),
                                    is(newLeader.getLeaderSessionID()));
                            assertThat(
                                    testingListener.getAddress(), is(newLeader.getLeaderAddress()));
                        });
            }
        };
    }

    @Test
    public void testNotifyLeaderAddressEmpty() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            testingListener.waitForNewLeader(timeout);

                            testingLeaderRetrievalDriver.onUpdate(LeaderInformation.empty());
                            testingListener.waitForEmptyLeaderInformation(timeout);
                            assertThat(testingListener.getLeaderSessionID(), is(nullValue()));
                            assertThat(testingListener.getAddress(), is(nullValue()));
                        });
            }
        };
    }

    @Test
    public void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test exception");

                            testingLeaderRetrievalDriver.onFatalError(testException);

                            testingListener.waitForError(timeout);
                            assertThat(
                                    testingListener.getError(),
                                    FlinkMatchers.containsCause(testException));
                        });
            }
        };
    }

    @Test
    public void testErrorIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test exception");

                            leaderRetrievalService.stop();
                            testingLeaderRetrievalDriver.onFatalError(testException);

                            try {
                                testingListener.waitForError(timeout);
                                fail(
                                        "We expect to have a timeout here because there's no error should be passed to listener.");
                            } catch (TimeoutException ex) {
                                // noop
                            }
                            assertThat(testingListener.getError(), is(nullValue()));
                        });
            }
        };
    }

    @Test
    public void testNotifyLeaderAddressOnlyWhenLeaderTrulyChanged() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation newLeader =
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL);
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            assertThat(testingListener.getLeaderEventQueueSize(), is(1));

                            // Same leader information should not be notified twice.
                            testingLeaderRetrievalDriver.onUpdate(newLeader);
                            assertThat(testingListener.getLeaderEventQueueSize(), is(1));

                            // Leader truly changed.
                            testingLeaderRetrievalDriver.onUpdate(
                                    LeaderInformation.known(UUID.randomUUID(), TEST_URL + 1));
                            assertThat(testingListener.getLeaderEventQueueSize(), is(2));
                        });
            }
        };
    }

    private class Context {
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
            assertThat(testingLeaderRetrievalDriver, is(notNullValue()));
            testMethod.run();

            leaderRetrievalService.stop();
        }
    }
}
