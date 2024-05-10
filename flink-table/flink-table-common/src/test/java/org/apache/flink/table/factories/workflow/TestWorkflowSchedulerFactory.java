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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.WorkflowSchedulerFactory;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** This class is an implementation of {@link WorkflowSchedulerFactory} for testing purposes. */
public class TestWorkflowSchedulerFactory implements WorkflowSchedulerFactory {

    public static final String IDENTIFIER = "test";

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("user-name").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();
    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key("project-name").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROJECT_NAME);
        return options;
    }

    @Override
    public WorkflowScheduler<?> createWorkflowScheduler(Context context) {
        WorkflowSchedulerFactoryUtil.WorkflowSchedulerFactoryHelper helper =
                WorkflowSchedulerFactoryUtil.createWorkflowSchedulerFactoryHelper(this, context);
        helper.validate();

        return new TestWorkflowScheduler(
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(PROJECT_NAME));
    }

    /** Test workflow scheduler for discovery testing. */
    public static class TestWorkflowScheduler implements WorkflowScheduler<TestRefreshHandler> {

        private final String userName;
        private final String password;
        private final String projectName;

        public TestWorkflowScheduler(String userName, String password, String projectName) {
            this.userName = userName;
            this.password = password;
            this.projectName = projectName;
        }

        @Override
        public void open() throws WorkflowException {}

        @Override
        public void close() throws WorkflowException {}

        @Override
        public RefreshHandlerSerializer<TestRefreshHandler> getRefreshHandlerSerializer() {
            return TestRefreshHandlerSerializer.INSTANCE;
        }

        @Override
        public TestRefreshHandler createRefreshWorkflow(CreateRefreshWorkflow createRefreshWorkflow)
                throws WorkflowException {
            return TestRefreshHandler.INSTANCE;
        }

        @Override
        public void modifyRefreshWorkflow(
                ModifyRefreshWorkflow<TestRefreshHandler> modifyRefreshWorkflow)
                throws WorkflowException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRefreshWorkflow(
                DeleteRefreshWorkflow<TestRefreshHandler> deleteRefreshWorkflow)
                throws WorkflowException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestWorkflowScheduler that = (TestWorkflowScheduler) o;
            return Objects.equals(userName, that.userName)
                    && Objects.equals(password, that.password)
                    && Objects.equals(projectName, that.projectName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(userName, password, projectName);
        }
    }

    /** Test refresh handler for discovery testing. */
    public static class TestRefreshHandler implements RefreshHandler {

        public static final TestRefreshHandler INSTANCE = new TestRefreshHandler();

        @Override
        public String asSummaryString() {
            return "Test RefreshHandler";
        }
    }

    /** Test refresh handler serializer for discovery testing. */
    public static class TestRefreshHandlerSerializer
            implements RefreshHandlerSerializer<TestRefreshHandler> {

        public static final TestRefreshHandlerSerializer INSTANCE =
                new TestRefreshHandlerSerializer();

        @Override
        public byte[] serialize(TestRefreshHandler refreshHandler) throws IOException {
            return new byte[0];
        }

        @Override
        public TestRefreshHandler deserialize(byte[] serializedBytes, ClassLoader cl)
                throws IOException, ClassNotFoundException {
            return TestRefreshHandler.INSTANCE;
        }
    }
}
