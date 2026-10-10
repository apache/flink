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

package org.apache.flink.container.entrypoint;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ApplicationModeMiniCluster}. */
class ApplicationModeMiniClusterTest {

    @RegisterExtension
    private final LoggerAuditingExtension loggerAuditingExtension =
            new LoggerAuditingExtension(ApplicationModeMiniCluster.class, Level.WARN);

    private MiniClusterConfiguration miniClusterConfiguration;
    private PackagedProgram program;

    @BeforeEach
    void setUp() throws ProgramInvocationException {
        miniClusterConfiguration = new MiniClusterConfiguration.Builder().build();
        program =
                PackagedProgram.newBuilder()
                        .setEntryPointClassName(NoOpProgram.class.getName())
                        .build();
    }

    @Test
    void rejectsNullProgram() {
        assertThatThrownBy(() -> new ApplicationModeMiniCluster(miniClusterConfiguration, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void futuresIncompleteOnConstruction() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        assertThat(miniCluster.getApplicationShutDownFuture()).isNotDone();
        assertThat(miniCluster.getCloseRequestedFuture()).isNotDone();
    }

    @Test
    void closeBeforeStartRecordsCloseRequestOnly() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        final CompletableFuture<Void> closeFuture = miniCluster.closeAsync();

        assertThat(closeFuture).isCompleted();
        assertThat(miniCluster.getCloseRequestedFuture()).isCompleted();
        assertThat(miniCluster.getApplicationShutDownFuture())
                .as("closing the cluster must not look like the application finishing")
                .isNotDone();
    }

    @Test
    void closeIsIdempotent() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);

        assertThat(miniCluster.closeAsync()).isCompleted();
        assertThat(miniCluster.closeAsync()).isCompleted();
        assertThat(miniCluster.getCloseRequestedFuture()).isCompleted();
        assertThat(miniCluster.getApplicationShutDownFuture()).isNotDone();
    }

    @Test
    void taskManagerFatalErrorIgnoredAfterCloseRequested() {
        final ApplicationModeMiniCluster miniCluster =
                new ApplicationModeMiniCluster(miniClusterConfiguration, program);
        miniCluster.closeAsync();

        miniCluster
                .createTaskManagerFatalErrorHandler(0)
                .onFatalError(new RuntimeException("error during shutdown"));

        assertThat(loggerAuditingExtension.getMessages()).isEmpty();
    }

    public static class NoOpProgram {
        public static void main(String[] args) {}
    }
}
