/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that populates {@link TestNameProvider} with the current test name. This is the
 * JUnit 5 counterpart of the JUnit 4 {@link TestNameProvider} {@link org.junit.rules.TestRule}.
 *
 * <p>This extension is registered globally via {@code
 * META-INF/services/org.junit.jupiter.api.extension.Extension}.
 */
public class TestNameProviderExtension implements BeforeEachCallback, AfterEachCallback {

    @Override
    public void beforeEach(ExtensionContext context) {
        TestNameProvider.setCurrentTestName(context.getDisplayName());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        TestNameProvider.clearCurrentTestName();
    }
}
