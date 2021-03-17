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

package org.apache.flink.client.cli;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.TestingClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the STOP command. */
public class CliFrontendStopWithSavepointTest extends CliFrontendTestBase {

    @BeforeClass
    public static void setup() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterClass
    public static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @Test
    public void testStopWithOnlyJobId() throws Exception {
        // test stop properly
        JobID jid = new JobID();
        String jidString = jid.toString();

        String[] parameters = {jidString};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(false));
                    assertNull(savepointDirectory);
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    public void testStopWithDefaultSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(false));
                    assertNull(savepointDirectory);
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    public void testStopWithExplicitSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-p", "test-target-dir", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(false));
                    assertThat(savepointDirectory, is("test-target-dir"));
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);
        stopWithSavepointLatch.await();
    }

    @Test
    public void testStopOnlyWithMaxWM() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-d", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(true));
                    assertNull(savepointDirectory);
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    public void testStopWithMaxWMAndDefaultSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-p", "-d", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(true));
                    assertNull(savepointDirectory);
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    public void testStopWithMaxWMAndExplicitSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-d", "-p", "test-target-dir", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) -> {
                    assertThat(jobID, is(jid));
                    assertThat(advanceToEndOfEventTime, is(true));
                    assertThat(savepointDirectory, is("test-target-dir"));
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test(expected = CliArgsException.class)
    public void testUnrecognizedOption() throws Exception {
        // test unrecognized option
        String[] parameters = {"-v", "-l"};
        Configuration configuration = getConfiguration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.stop(parameters);
    }

    @Test(expected = CliArgsException.class)
    public void testMissingJobId() throws Exception {
        // test missing job id
        String[] parameters = {};
        Configuration configuration = getConfiguration();
        CliFrontend testFrontend =
                new CliFrontend(configuration, Collections.singletonList(getCli()));
        testFrontend.stop(parameters);
    }

    @Test(expected = CliArgsException.class)
    public void testWrongSavepointDirOrder() throws Exception {
        JobID jid = new JobID();
        String[] parameters = {"-s", "-d", "test-target-dir", jid.toString()};
        MockedCliFrontend testFrontend = new MockedCliFrontend(new TestingClusterClient());
        testFrontend.stop(parameters);
    }

    @Test
    public void testUnknownJobId() throws Exception {
        // test unknown job Id
        JobID jid = new JobID();

        String[] parameters = {"-p", "test-target-dir", jid.toString()};
        String expectedMessage = "Test exception";
        FlinkException testException = new FlinkException(expectedMessage);
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory) ->
                        FutureUtils.completedExceptionally(testException));
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

        try {
            testFrontend.stop(parameters);
            fail("Should have failed.");
        } catch (FlinkException e) {
            assertTrue(ExceptionUtils.findThrowableWithMessage(e, expectedMessage).isPresent());
        }
    }
}
