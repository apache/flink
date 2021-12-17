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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nullable;

/**
 * A rule that provides the current test name per thread. Currently, the test name is available for
 * all tests that extend {@link TestLogger}.
 */
public class TestNameProvider implements TestRule {
    private static ThreadLocal<String> testName = new ThreadLocal<>();

    @Nullable
    public static String getCurrentTestName() {
        return testName.get();
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                testName.set(description.getDisplayName());
                try {
                    base.evaluate();
                } finally {
                    testName.set(null);
                }
            }
        };
    }
}
