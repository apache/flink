/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Created} state. */
class CreatedTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreatedTest.class);

    @RegisterExtension MockCreatedContext ctx = new MockCreatedContext();

    @Test
    void testStartScheduling() {
        Created created = new Created(ctx, LOG);

        ctx.setExpectWaitingForResources();

        created.startScheduling();
    }

    @Test
    void testJobInformation() {
        Created created = new Created(ctx, LOG);
        ArchivedExecutionGraph job = created.getJob();
        assertThat(job.getState()).isEqualTo(JobStatus.INITIALIZING);
    }

    static class MockCreatedContext extends MockStateWithoutExecutionGraphContext
            implements Created.Context {
        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
            waitingForResourcesStateValidator.validateInput(null);
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            super.afterEach(extensionContext);
            waitingForResourcesStateValidator.close();
        }
    }
}
