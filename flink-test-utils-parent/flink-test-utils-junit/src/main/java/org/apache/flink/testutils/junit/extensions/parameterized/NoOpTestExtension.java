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

package org.apache.flink.testutils.junit.extensions.parameterized;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import java.util.stream.Stream;

/**
 * A special {@link TestTemplateInvocationContextProvider} that return default {@link
 * TestTemplateInvocationContext}.
 *
 * <p>When use this extension, all tests must be annotated by {@link TestTemplate}.
 */
public class NoOpTestExtension implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext extensionContext) {
        return Stream.of(DefaultTestTemplateInvocationContext.INSTANCE);
    }

    private static class DefaultTestTemplateInvocationContext
            implements TestTemplateInvocationContext {
        public static final DefaultTestTemplateInvocationContext INSTANCE =
                new DefaultTestTemplateInvocationContext();

        private DefaultTestTemplateInvocationContext() {}

        // no-op as base class has default implementation.
    }
}
