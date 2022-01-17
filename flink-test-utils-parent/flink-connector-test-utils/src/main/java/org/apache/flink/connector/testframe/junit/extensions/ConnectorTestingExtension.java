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

package org.apache.flink.connector.testframe.junit.extensions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.testframe.TestResource;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.ExternalContextFactory;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A JUnit 5 {@link Extension} for supporting running of connector testing framework.
 *
 * <p>This extension is responsible for searching test resources annotated by {@link TestEnv},
 * {@link TestExternalSystem} and {@link TestContext}, storing them into storage provided by JUnit,
 * and manage lifecycle of these resources.
 *
 * <p>The extension uses {@link ExtensionContext.Store} for handing over test resources to {@link
 * TestCaseInvocationContextProvider}, which will inject these resources into test cases as
 * parameters.
 *
 * <p>The order of initialization is promised to be:
 *
 * <ol>
 *   <li>Test environment annotated by {@link TestEnv}, before all test cases in this extension
 *   <li>External system annotated by {@link TestExternalSystem}, before all test cases in this
 *       extension
 *   <li>External contexts annotated by {@link TestContext}, before each test case in {@link
 *       TestCaseInvocationContextProvider}
 * </ol>
 */
@Internal
public class ConnectorTestingExtension implements BeforeAllCallback, AfterAllCallback {

    public static final ExtensionContext.Namespace TEST_RESOURCE_NAMESPACE =
            ExtensionContext.Namespace.create("testResourceNamespace");
    public static final String TEST_ENV_STORE_KEY = "testEnvironment";
    public static final String EXTERNAL_SYSTEM_STORE_KEY = "externalSystem";
    public static final String EXTERNAL_CONTEXT_FACTORIES_STORE_KEY = "externalContext";

    private TestEnvironment testEnvironment;
    private TestResource externalSystem;

    @SuppressWarnings("rawtypes")
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        // Setup test environment and store
        final List<TestEnvironment> testEnvironments =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(), TestEnv.class, TestEnvironment.class);
        checkExactlyOneAnnotatedField(testEnvironments, TestEnv.class);
        testEnvironment = testEnvironments.get(0);
        testEnvironment.startUp();
        context.getStore(TEST_RESOURCE_NAMESPACE).put(TEST_ENV_STORE_KEY, testEnvironment);

        // Setup external system and store
        final List<TestResource> externalSystems =
                findAnnotatedFieldValues(context, TestExternalSystem.class, TestResource.class);
        if (!externalSystems.isEmpty()) {
            externalSystem = externalSystems.get(0);
            externalSystem.startUp();
            context.getStore(TEST_RESOURCE_NAMESPACE)
                    .put(EXTERNAL_SYSTEM_STORE_KEY, externalSystem);
        }

        // Search external context factories
        final List<ExternalContextFactory> externalContextFactories =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(),
                        TestContext.class,
                        ExternalContextFactory.class);
        checkAtLeastOneAnnotationField(externalContextFactories, TestContext.class);
        context.getStore(TEST_RESOURCE_NAMESPACE)
                .put(EXTERNAL_CONTEXT_FACTORIES_STORE_KEY, externalContextFactories);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        // Tear down test environment
        testEnvironment.tearDown();

        // Tear down external system
        if (externalSystem != null) {
            externalSystem.tearDown();
        }

        // Clear store
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(TEST_ENV_STORE_KEY);
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(EXTERNAL_SYSTEM_STORE_KEY);
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(EXTERNAL_CONTEXT_FACTORIES_STORE_KEY);
    }

    private <T> List<T> findAnnotatedFieldValues(
            ExtensionContext context,
            Class<? extends Annotation> annotationType,
            Class<T> fieldType) {
        final List<T> fields = new ArrayList<>();
        fields.addAll(
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(), annotationType, fieldType));
        fields.addAll(
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestClass(), annotationType, fieldType));
        return fields;
    }

    private void checkExactlyOneAnnotatedField(
            Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple fields are annotated with '@%s'",
                            annotation.getSimpleName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }

    private void checkAtLeastOneAnnotationField(
            Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }
}
