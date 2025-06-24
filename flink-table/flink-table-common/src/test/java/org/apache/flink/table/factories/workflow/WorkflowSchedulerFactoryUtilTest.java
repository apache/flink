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

package org.apache.flink.table.factories.workflow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.WorkflowSchedulerFactory;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil.createWorkflowScheduler;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link WorkflowSchedulerFactoryUtil}. */
public class WorkflowSchedulerFactoryUtilTest {

    @Test
    void testCreateWorkflowScheduler() {
        final Map<String, String> options = getDefaultConfig();
        WorkflowScheduler<?> actual =
                createWorkflowScheduler(
                        Configuration.fromMap(options),
                        Thread.currentThread().getContextClassLoader());

        WorkflowScheduler<?> expected =
                new TestWorkflowSchedulerFactory.TestWorkflowScheduler("user1", "9999", "project1");

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testCreateWorkflowSchedulerWithoutType() {
        WorkflowScheduler<?> actual =
                createWorkflowScheduler(
                        new Configuration(), Thread.currentThread().getContextClassLoader());

        assertThat(actual).isNull();
    }

    @Test
    void testCreateWorkflowSchedulerWithUnknownType() {
        final Map<String, String> options = getDefaultConfig();
        options.put("workflow-scheduler.type", "unknown");

        validateException(
                options,
                String.format(
                        "Could not find any factory for identifier 'unknown' "
                                + "that implements '%s' in the classpath.",
                        WorkflowSchedulerFactory.class.getCanonicalName()));
    }

    @Test
    void testCreateWorkflowSchedulerWithMissingOptions() {
        final Map<String, String> options = getDefaultConfig();
        options.remove("workflow-scheduler.test.user-name");

        validateException(
                options,
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "user-name");
    }

    // --------------------------------------------------------------------------------------------

    private void validateException(Map<String, String> options, String errorMessage) {
        assertThatThrownBy(
                        () ->
                                createWorkflowScheduler(
                                        Configuration.fromMap(options),
                                        Thread.currentThread().getContextClassLoader()))
                .satisfies(anyCauseMatches(ValidationException.class, errorMessage));
    }

    private Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("workflow-scheduler.type", "test");
        config.put("workflow-scheduler.test.user-name", "user1");
        config.put("workflow-scheduler.test.password", "9999");
        config.put("workflow-scheduler.test.project-name", "project1");
        return config;
    }
}
