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

package org.apache.flink.tests.util;

/**
 * Resource lifecycle interface for end-to-end tests.
 *
 * <p>This interface provides hooks for resource setup and cleanup, allowing resources to
 * differentiate between successful and failed tests in their cleanup methods.
 *
 * <p>This is a JUnit5-compatible version that does not extend TestRule.
 */
public interface ExternalResource {

    /** Called before the test execution. */
    void before() throws Exception;

    /** Called after successful test execution. */
    void afterTestSuccess();

    /** Called after failed test execution. Defaults to calling {@link #afterTestSuccess()}. */
    default void afterTestFailure() {
        afterTestSuccess();
    }
}
