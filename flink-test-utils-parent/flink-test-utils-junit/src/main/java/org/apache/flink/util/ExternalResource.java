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

package org.apache.flink.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Modified version of the jUnit {@link org.junit.rules.ExternalResource}.
 *
 * <p>This version is an interface instead of an abstract class and allows resources to
 * differentiate between successful and failed tests in their {@code After} methods.
 */
public interface ExternalResource extends TestRule {

    void before() throws Exception;

    void afterTestSuccess();

    default void afterTestFailure() {
        afterTestSuccess();
    }

    @Override
    default Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                try {
                    base.evaluate();
                } catch (final Throwable testThrowable) {
                    try {
                        afterTestFailure();
                    } catch (final Throwable afterFailureThrowable) {
                        testThrowable.addSuppressed(afterFailureThrowable);
                    }
                    throw testThrowable;
                }
                afterTestSuccess();
            }
        };
    }
}
