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
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the SAVEPOINT command. */
class CliFrontendSavepointTest extends CliFrontendTestBase {

    private PrintStream stdOut;
    private PrintStream stdErr;
    private ByteArrayOutputStream buffer;

    // ------------------------------------------------------------------------
    // Trigger savepoint
    // ------------------------------------------------------------------------

    @Test
    void testTriggerSavepointSuccess() throws Exception {

        JobID jobId = new JobID();

        String savepointPath = "expectedSavepointPath";

        final ClusterClient<String> clusterClient = createClusterClient(savepointPath);

        try {
            MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {jobId.toString()};
            frontend.savepoint(parameters);

            verify(clusterClient, times(1))
                    .triggerSavepoint(eq(jobId), isNull(), eq(SavepointFormatType.DEFAULT));

            assertThat(buffer.toString()).contains(savepointPath);
        } finally {
            clusterClient.close();
        }
    }

    @Test
    void testTriggerSavepointFailure() {

        JobID jobId = new JobID();

        String expectedTestException = "expectedTestException";
        Exception testException = new Exception(expectedTestException);

        try (ClusterClient<String> clusterClient = createFailingClusterClient(testException)) {
            MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {jobId.toString()};

            assertThatThrownBy(() -> frontend.savepoint(parameters))
                    .isInstanceOf(FlinkException.class)
                    .hasRootCause(testException);
        }
    }

    @Test
    void testTriggerSavepointFailureIllegalJobID() throws Exception {

        CliFrontend frontend =
                new MockedCliFrontend(
                        new RestClusterClient<>(
                                getConfiguration(), StandaloneClusterId.getInstance()));

        String[] parameters = {"invalid job id"};
        assertThatThrownBy(() -> frontend.savepoint(parameters))
                .isInstanceOf(CliArgsException.class)
                .hasMessageContaining("Cannot parse JobID");
    }

    /**
     * Tests that a CLI call with a custom savepoint directory target is forwarded correctly to the
     * cluster client.
     */
    @Test
    void testTriggerSavepointCustomTarget() throws Exception {

        JobID jobId = new JobID();

        String savepointDirectory = "customTargetDirectory";

        final ClusterClient<String> clusterClient = createClusterClient(savepointDirectory);

        try {
            MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {jobId.toString(), savepointDirectory};
            frontend.savepoint(parameters);

            verify(clusterClient, times(1))
                    .triggerSavepoint(
                            eq(jobId), eq(savepointDirectory), eq(SavepointFormatType.DEFAULT));

            assertThat(buffer.toString()).contains(savepointDirectory);
        } finally {
            clusterClient.close();
        }
    }

    @CsvSource({"-type, NATIVE", "--type, NATIVE"})
    @ParameterizedTest
    void testTriggerSavepointCustomFormat(String flag, SavepointFormatType formatType)
            throws Exception {

        JobID jobId = new JobID();

        String savepointDirectory = "customTargetDirectory";

        final ClusterClient<String> clusterClient = createClusterClient(savepointDirectory);

        try {
            MockedCliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {
                jobId.toString(), savepointDirectory, flag, formatType.toString()
            };
            frontend.savepoint(parameters);

            verify(clusterClient, times(1))
                    .triggerSavepoint(eq(jobId), eq(savepointDirectory), eq(formatType));

            assertThat(buffer.toString()).contains(savepointDirectory);
        } finally {
            clusterClient.close();
        }
    }

    // ------------------------------------------------------------------------
    // Dispose savepoint
    // ------------------------------------------------------------------------

    @Test
    void testDisposeSavepointSuccess() throws Exception {

        String savepointPath = "expectedSavepointPath";

        ClusterClient clusterClient =
                new DisposeSavepointClusterClient(
                        (String path) -> CompletableFuture.completedFuture(Acknowledge.get()),
                        getConfiguration());

        try {

            CliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {"-d", savepointPath};
            frontend.savepoint(parameters);

            String outMsg = buffer.toString();
            assertThat(outMsg).contains(savepointPath, "disposed");
        } finally {
            clusterClient.close();
        }
    }

    /** Tests disposal with a JAR file. */
    @Test
    void testDisposeWithJar(@TempDir java.nio.file.Path tmp) throws Exception {

        final CompletableFuture<String> disposeSavepointFuture = new CompletableFuture<>();

        final DisposeSavepointClusterClient clusterClient =
                new DisposeSavepointClusterClient(
                        (String savepointPath) -> {
                            disposeSavepointFuture.complete(savepointPath);
                            return CompletableFuture.completedFuture(Acknowledge.get());
                        },
                        getConfiguration());

        try {
            CliFrontend frontend = new MockedCliFrontend(clusterClient);

            // Fake JAR file
            File f = tmp.resolve("test.jar").toFile();
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
            out.close();

            final String disposePath = "any-path";
            String[] parameters = {"-d", disposePath, "-j", f.toPath().toAbsolutePath().toString()};

            frontend.savepoint(parameters);

            final String actualSavepointPath = disposeSavepointFuture.get();

            assertThat(actualSavepointPath).isEqualTo(disposePath);
        } finally {
            clusterClient.close();
        }
    }

    @Test
    void testDisposeSavepointFailure() throws Exception {

        String savepointPath = "expectedSavepointPath";

        Exception testException = new Exception("expectedTestException");

        try (DisposeSavepointClusterClient clusterClient =
                new DisposeSavepointClusterClient(
                        (String path) -> FutureUtils.completedExceptionally(testException),
                        getConfiguration())) {
            CliFrontend frontend = new MockedCliFrontend(clusterClient);

            String[] parameters = {"-d", savepointPath};

            assertThatThrownBy(() -> frontend.savepoint(parameters))
                    .isInstanceOf(Exception.class)
                    .hasRootCause(testException);
        }
    }

    // ------------------------------------------------------------------------

    private static final class DisposeSavepointClusterClient
            extends RestClusterClient<StandaloneClusterId> {

        private final Function<String, CompletableFuture<Acknowledge>> disposeSavepointFunction;

        DisposeSavepointClusterClient(
                Function<String, CompletableFuture<Acknowledge>> disposeSavepointFunction,
                Configuration configuration)
                throws Exception {
            super(configuration, StandaloneClusterId.getInstance());

            this.disposeSavepointFunction = Preconditions.checkNotNull(disposeSavepointFunction);
        }

        @Override
        public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
            return disposeSavepointFunction.apply(savepointPath);
        }
    }

    @BeforeEach
    void replaceStdOutAndStdErr() {
        stdOut = System.out;
        stdErr = System.err;
        buffer = new ByteArrayOutputStream();
        PrintStream capture = new PrintStream(buffer);
        System.setOut(capture);
        System.setErr(capture);
    }

    @AfterEach
    void restoreStdOutAndStdErr() {
        System.setOut(stdOut);
        System.setErr(stdErr);
    }

    private static ClusterClient<String> createClusterClient(String expectedResponse) {
        final ClusterClient<String> clusterClient = mock(ClusterClient.class);

        when(clusterClient.triggerSavepoint(
                        any(JobID.class),
                        nullable(String.class),
                        nullable(SavepointFormatType.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        return clusterClient;
    }

    private static ClusterClient<String> createFailingClusterClient(Exception expectedException) {
        final ClusterClient<String> clusterClient = mock(ClusterClient.class);

        when(clusterClient.triggerSavepoint(
                        any(JobID.class),
                        nullable(String.class),
                        nullable(SavepointFormatType.class)))
                .thenReturn(FutureUtils.completedExceptionally(expectedException));

        return clusterClient;
    }
}
