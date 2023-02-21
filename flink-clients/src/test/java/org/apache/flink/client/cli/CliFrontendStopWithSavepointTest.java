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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the STOP command. */
class CliFrontendStopWithSavepointTest extends CliFrontendTestBase {

    @BeforeAll
    static void setup() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @Test
    void testStopWithOnlyJobId() throws Exception {
        // test stop properly
        JobID jid = new JobID();
        String jidString = jid.toString();

        String[] parameters = {jidString};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isFalse();
                    assertThat(savepointDirectory).isNull();
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    void testStopWithDefaultSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isFalse();
                    assertThat(savepointDirectory).isNull();
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    void testStopWithExplicitSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-p", "test-target-dir", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isFalse();
                    assertThat(savepointDirectory).isEqualTo("test-target-dir");
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);
        stopWithSavepointLatch.await();
    }

    @CsvSource(value = {"-type, NATIVE", "--type, NATIVE"})
    @ParameterizedTest
    void testStopWithExplicitSavepointType(String flag, SavepointFormatType expectedFormat)
            throws Exception {
        JobID jid = new JobID();

        String[] parameters = {
            "-p", "test-target-dir", jid.toString(), flag, expectedFormat.toString()
        };
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isFalse();
                    assertThat(savepointDirectory).isEqualTo("test-target-dir");
                    assertThat(formatType).isEqualTo(expectedFormat);
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);
        stopWithSavepointLatch.await();
    }

    @Test
    void testStopOnlyWithMaxWM() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-d", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isTrue();
                    assertThat(savepointDirectory).isNull();
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    void testStopWithMaxWMAndDefaultSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-p", "-d", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isTrue();
                    assertThat(savepointDirectory).isNull();
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    void testStopWithMaxWMAndExplicitSavepointDir() throws Exception {
        JobID jid = new JobID();

        String[] parameters = {"-d", "-p", "test-target-dir", jid.toString()};
        OneShotLatch stopWithSavepointLatch = new OneShotLatch();
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) -> {
                    assertThat(jobID).isEqualTo(jid);
                    assertThat(advanceToEndOfEventTime).isTrue();
                    assertThat(savepointDirectory).isEqualTo("test-target-dir");
                    stopWithSavepointLatch.trigger();
                    return CompletableFuture.completedFuture(savepointDirectory);
                });
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
        testFrontend.stop(parameters);

        stopWithSavepointLatch.await();
    }

    @Test
    void testUnrecognizedOption() {
        assertThatThrownBy(
                        () -> {
                            // test unrecognized option
                            String[] parameters = {"-v", "-l"};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.stop(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testMissingJobId() {
        assertThatThrownBy(
                        () -> {
                            // test missing job id
                            String[] parameters = {};
                            Configuration configuration = getConfiguration();
                            CliFrontend testFrontend =
                                    new CliFrontend(
                                            configuration, Collections.singletonList(getCli()));
                            testFrontend.stop(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testWrongSavepointDirOrder() {
        assertThatThrownBy(
                        () -> {
                            JobID jid = new JobID();
                            String[] parameters = {"-s", "-d", "test-target-dir", jid.toString()};
                            MockedCliFrontend testFrontend =
                                    new MockedCliFrontend(new TestingClusterClient());
                            testFrontend.stop(parameters);
                        })
                .isInstanceOf(CliArgsException.class);
    }

    @Test
    void testUnknownJobId() {
        // test unknown job Id
        JobID jid = new JobID();

        String[] parameters = {"-p", "test-target-dir", jid.toString()};
        String expectedMessage = "Test exception";
        FlinkException testException = new FlinkException(expectedMessage);
        TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
        clusterClient.setStopWithSavepointFunction(
                (jobID, advanceToEndOfEventTime, savepointDirectory, formatType) ->
                        FutureUtils.completedExceptionally(testException));
        MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

        assertThatThrownBy(() -> testFrontend.stop(parameters))
                .isInstanceOf(FlinkException.class)
                .hasRootCause(testException);
    }
}
