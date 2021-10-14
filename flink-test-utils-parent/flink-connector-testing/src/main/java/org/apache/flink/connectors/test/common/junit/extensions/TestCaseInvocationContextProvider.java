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

package org.apache.flink.connectors.test.common.junit.extensions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension.EXTERNAL_CONTEXT_FACTORIES_STORE_KEY;
import static org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension.TEST_ENV_STORE_KEY;
import static org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension.TEST_RESOURCE_NAMESPACE;

/**
 * A helper class for injecting test resources into test case as parameters.
 *
 * <p>This provider will resolve {@link TestEnvironment} and {@link ExternalContext.Factory} from
 * the storage in JUnit's {@link ExtensionContext}, inject them into test method, and register a
 * {@link AfterTestExecutionCallback} for closing the external context after the execution of test
 * case.
 */
@Internal
public class TestCaseInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        // Only support test cases with TestEnvironment and ExternalContext as parameter
        return Arrays.stream(context.getRequiredTestMethod().getParameterTypes())
                .anyMatch(
                        (type) ->
                                TestEnvironment.class.isAssignableFrom(type)
                                        || ExternalContext.class.isAssignableFrom(type));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {

        // Fetch test environment from store
        TestEnvironment testEnv =
                context.getStore(TEST_RESOURCE_NAMESPACE)
                        .get(TEST_ENV_STORE_KEY, TestEnvironment.class);

        // Fetch external context factories from store
        List<ExternalContext.Factory<?>> externalContextFactories =
                (List<ExternalContext.Factory<?>>)
                        context.getStore(TEST_RESOURCE_NAMESPACE)
                                .get(EXTERNAL_CONTEXT_FACTORIES_STORE_KEY);

        // Create a invocation context for each external context factory
        return externalContextFactories.stream()
                .map(
                        factory ->
                                new TestResourceProvidingInvocationContext(
                                        testEnv, factory.createExternalContext()));
    }

    /**
     * Invocation context for injecting {@link TestEnvironment} and {@link ExtensionContext} into
     * test cases as method parameters.
     */
    static class TestResourceProvidingInvocationContext implements TestTemplateInvocationContext {

        private final TestEnvironment testEnvironment;
        private final ExternalContext<?> externalContext;

        public TestResourceProvidingInvocationContext(
                TestEnvironment testEnvironment, ExternalContext<?> externalContext) {
            this.testEnvironment = testEnvironment;
            this.externalContext = externalContext;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format(
                    "TestEnvironment: [%s], ExternalContext: [%s]",
                    testEnvironment, externalContext);
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                    // Extension for injecting parameters
                    new TestEnvironmentResolver(testEnvironment),
                    new ExternalContextProvider(externalContext),
                    new ClusterControllableProvider(testEnvironment),
                    // Extension for closing external context
                    (AfterTestExecutionCallback) ignore -> externalContext.close());
        }
    }

    private static class TestEnvironmentResolver implements ParameterResolver {

        private final TestEnvironment testEnvironment;

        private TestEnvironmentResolver(TestEnvironment testEnvironment) {
            this.testEnvironment = testEnvironment;
        }

        @Override
        public boolean supportsParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return isAssignableFromParameterType(
                    TestEnvironment.class, parameterContext.getParameter().getType());
        }

        @Override
        public Object resolveParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return this.testEnvironment;
        }
    }

    private static class ExternalContextProvider implements ParameterResolver {

        private final ExternalContext<?> externalContext;

        private ExternalContextProvider(ExternalContext<?> externalContext) {
            this.externalContext = externalContext;
        }

        @Override
        public boolean supportsParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return isAssignableFromParameterType(
                    ExternalContext.class, parameterContext.getParameter().getType());
        }

        @Override
        public Object resolveParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return this.externalContext;
        }
    }

    private static class ClusterControllableProvider implements ParameterResolver {

        private final TestEnvironment testEnvironment;

        private ClusterControllableProvider(TestEnvironment testEnvironment) {
            this.testEnvironment = testEnvironment;
        }

        @Override
        public boolean supportsParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return isAssignableFromParameterType(
                            ClusterControllable.class, parameterContext.getParameter().getType())
                    && isTestEnvironmentControllable(this.testEnvironment);
        }

        @Override
        public Object resolveParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return testEnvironment;
        }

        private boolean isTestEnvironmentControllable(TestEnvironment testEnvironment) {
            return ClusterControllable.class.isAssignableFrom(testEnvironment.getClass());
        }
    }

    private static boolean isAssignableFromParameterType(
            Class<?> requiredType, Class<?> parameterType) {
        return requiredType.isAssignableFrom(parameterType);
    }
}
