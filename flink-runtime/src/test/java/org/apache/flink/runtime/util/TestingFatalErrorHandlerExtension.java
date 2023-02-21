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

package org.apache.flink.runtime.util;

import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;

/** {@link TestingFatalErrorHandler} extension for Junit5. */
public class TestingFatalErrorHandlerExtension implements BeforeEachCallback, AfterEachCallback {

    @Nullable private TestingFatalErrorHandler testingFatalErrorHandler;

    public TestingFatalErrorHandler getTestingFatalErrorHandler() {
        return Preconditions.checkNotNull(
                testingFatalErrorHandler,
                "The %s has not been properly initialized.",
                TestingFatalErrorHandlerExtension.class.getSimpleName());
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // instantiate a fresh fatal error handler
        testingFatalErrorHandler = new TestingFatalErrorHandler();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        final Throwable exception = getTestingFatalErrorHandler().getException();

        if (exception != null) {
            throw new AssertionError(
                    "The TestingFatalErrorHandler caught an exception.", exception);
        }
    }
}
