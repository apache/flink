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

package org.apache.flink.tests.util.flink;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that wraps a {@link FlinkResource} and manages its lifecycle. Registered on a
 * static field, the resource is shared by all tests of the class; otherwise it is per test.
 */
public class FlinkResourceExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private final FlinkResource delegate;

    private boolean perClass;
    private boolean anyTestFailed;

    public FlinkResourceExtension(FlinkResource delegate) {
        this.delegate = delegate;
    }

    public FlinkResource getFlinkResource() {
        return delegate;
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        perClass = true;
        delegate.before();
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (anyTestFailed || context.getExecutionException().isPresent()) {
            delegate.afterTestFailure();
        } else {
            delegate.afterTestSuccess();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (!perClass) {
            delegate.before();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (perClass) {
            anyTestFailed |= context.getExecutionException().isPresent();
        } else if (context.getExecutionException().isPresent()) {
            delegate.afterTestFailure();
        } else {
            delegate.afterTestSuccess();
        }
    }
}
