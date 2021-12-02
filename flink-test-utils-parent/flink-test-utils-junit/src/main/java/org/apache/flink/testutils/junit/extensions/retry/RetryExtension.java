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

package org.apache.flink.testutils.junit.extensions.retry;

import org.apache.flink.testutils.junit.RetryOnException;
import org.apache.flink.testutils.junit.RetryOnFailure;
import org.apache.flink.testutils.junit.extensions.retry.strategy.RetryOnExceptionStrategy;
import org.apache.flink.testutils.junit.extensions.retry.strategy.RetryOnFailureStrategy;
import org.apache.flink.testutils.junit.extensions.retry.strategy.RetryStrategy;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** An extension to let failed test retry. */
public class RetryExtension implements TestTemplateInvocationContextProvider, AfterAllCallback {
    static final ExtensionContext.Namespace RETRY_NAMESPACE =
            ExtensionContext.Namespace.create("retryLog");
    static final String RETRY_KEY = "testRetry";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        RetryOnFailure retryOnFailure = getRetryAnnotation(context, RetryOnFailure.class);
        RetryOnException retryOnException = getRetryAnnotation(context, RetryOnException.class);
        return retryOnException != null || retryOnFailure != null;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {
        RetryOnFailure retryOnFailure = getRetryAnnotation(context, RetryOnFailure.class);
        RetryOnException retryOnException = getRetryAnnotation(context, RetryOnException.class);

        // sanity check that we don't use both annotations
        if (retryOnFailure != null && retryOnException != null) {
            throw new IllegalArgumentException(
                    "You cannot combine the RetryOnFailure and RetryOnException annotations.");
        }

        Map<String, RetryStrategy> testLog =
                (Map<String, RetryStrategy>)
                        context.getStore(RETRY_NAMESPACE)
                                .getOrComputeIfAbsent(RETRY_KEY, key -> new HashMap<>());
        int totalTimes;
        if (retryOnException != null) {
            totalTimes = retryOnException.times() + 1;
            testLog.put(
                    getTestMethodKey(context),
                    new RetryOnExceptionStrategy(totalTimes, retryOnException.exception()));
        } else if (retryOnFailure != null) {
            totalTimes = retryOnFailure.times() + 1;
            testLog.put(getTestMethodKey(context), new RetryOnFailureStrategy(totalTimes));
        } else {
            throw new IllegalArgumentException("Unsupported retry strategy.");
        }

        return IntStream.rangeClosed(1, totalTimes).mapToObj(i -> new RetryContext(i, totalTimes));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        context.getStore(RETRY_NAMESPACE).remove(RETRY_KEY);
    }

    static String getTestMethodKey(ExtensionContext context) {
        return context.getRequiredTestClass().getCanonicalName()
                + "#"
                + context.getRequiredTestMethod().getName();
    }

    private <T extends Annotation> T getRetryAnnotation(
            ExtensionContext context, Class<T> annotationClass) {
        Method testMethod = context.getRequiredTestMethod();
        T annotation = testMethod.getAnnotation(annotationClass);

        if (annotation == null) {
            // if nothing is specified on the test method, fall back to annotations on the class
            annotation = context.getTestClass().get().getAnnotation(annotationClass);
        }
        return annotation;
    }

    class RetryContext implements TestTemplateInvocationContext {
        final int retryIndex;
        final int totalTimes;

        RetryContext(int retryIndex, int totalTimes) {
            this.totalTimes = totalTimes;
            this.retryIndex = retryIndex;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format("Attempt [%d/%d]", retryIndex, totalTimes);
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(new RetryTestExecutionExtension(retryIndex, totalTimes));
        }
    }
}
