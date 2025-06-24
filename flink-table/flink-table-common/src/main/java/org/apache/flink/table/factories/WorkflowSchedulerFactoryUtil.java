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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.workflow.WorkflowScheduler;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;
import static org.apache.flink.table.factories.FactoryUtil.WORKFLOW_SCHEDULER_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.stringifyOption;

/** Utility for working with {@link WorkflowScheduler}. */
@PublicEvolving
public class WorkflowSchedulerFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSchedulerFactoryUtil.class);

    public static final String WORKFLOW_SCHEDULER_PREFIX = "workflow-scheduler";

    private WorkflowSchedulerFactoryUtil() {
        // no instantiation
    }

    /**
     * Attempts to discover the appropriate workflow scheduler factory and creates the instance of
     * the scheduler. Return null directly if doesn't specify the workflow scheduler in config
     * because it is optional for materialized table.
     */
    public static @Nullable WorkflowScheduler<?> createWorkflowScheduler(
            Configuration configuration, ClassLoader classLoader) {
        // Workflow scheduler identifier
        String identifier = configuration.get(WORKFLOW_SCHEDULER_TYPE);
        if (StringUtils.isNullOrWhitespaceOnly(identifier)) {
            LOG.warn(
                    "Workflow scheduler options do not contain an option key '{}' for discovering an workflow scheduler.",
                    WORKFLOW_SCHEDULER_TYPE.key());
            return null;
        }

        try {
            final WorkflowSchedulerFactory factory =
                    FactoryUtil.discoverFactory(
                            classLoader, WorkflowSchedulerFactory.class, identifier);
            return factory.createWorkflowScheduler(
                    new DefaultWorkflowSchedulerContext(
                            configuration, getWorkflowSchedulerConfig(configuration, identifier)));
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "Error creating workflow scheduler '%s' in option space '%s'.",
                            identifier,
                            configuration.toMap().entrySet().stream()
                                    .map(
                                            optionEntry ->
                                                    stringifyOption(
                                                            optionEntry.getKey(),
                                                            optionEntry.getValue()))
                                    .sorted()
                                    .collect(Collectors.joining("\n"))),
                    t);
        }
    }

    private static Map<String, String> getWorkflowSchedulerConfig(
            Configuration flinkConf, String identifier) {
        return new DelegatingConfiguration(flinkConf, getWorkflowSchedulerOptionPrefix(identifier))
                .toMap();
    }

    private static String getWorkflowSchedulerOptionPrefix(String identifier) {
        return String.format("%s.%s.", WORKFLOW_SCHEDULER_PREFIX, identifier);
    }

    /**
     * Creates a utility that helps to validate options for a {@link WorkflowSchedulerFactory}.
     *
     * <p>Note: This utility checks for left-over options in the final step.
     */
    public static WorkflowSchedulerFactoryHelper createWorkflowSchedulerFactoryHelper(
            WorkflowSchedulerFactory workflowSchedulerFactory,
            WorkflowSchedulerFactory.Context context) {
        return new WorkflowSchedulerFactoryHelper(
                workflowSchedulerFactory, context.getWorkflowSchedulerOptions());
    }

    /**
     * Helper utility for validating all options for a {@link WorkflowSchedulerFactory}.
     *
     * @see #createWorkflowSchedulerFactoryHelper(WorkflowSchedulerFactory,
     *     WorkflowSchedulerFactory.Context)
     */
    @PublicEvolving
    public static class WorkflowSchedulerFactoryHelper
            extends FactoryUtil.FactoryHelper<WorkflowSchedulerFactory> {

        public WorkflowSchedulerFactoryHelper(
                WorkflowSchedulerFactory workflowSchedulerFactory,
                Map<String, String> configOptions) {
            super(workflowSchedulerFactory, configOptions, PROPERTY_VERSION);
        }
    }

    /** Default implementation of {@link WorkflowSchedulerFactory.Context}. */
    @Internal
    public static class DefaultWorkflowSchedulerContext
            implements WorkflowSchedulerFactory.Context {

        private final ReadableConfig configuration;
        private final Map<String, String> workflowSchedulerConfig;

        public DefaultWorkflowSchedulerContext(
                ReadableConfig configuration, Map<String, String> workflowSchedulerConfig) {
            this.configuration = configuration;
            this.workflowSchedulerConfig = workflowSchedulerConfig;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return configuration;
        }

        @Override
        public Map<String, String> getWorkflowSchedulerOptions() {
            return workflowSchedulerConfig;
        }
    }
}
