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

package org.apache.flink.table.test.program;

/**
 * Test step that makes up a {@link TableTestProgram}.
 *
 * <p>It describes a task that should be executed either before running the actual test or as the
 * main ingredient of the test.
 *
 * <p>Some steps provide {@code apply()} methods for convenience. But in the end, the {@link
 * TableTestProgramRunner} decides whether to call them or not.
 *
 * <p>Not every {@link TableTestProgramRunner} might support every {@link TestKind}.
 */
public interface TestStep {

    /**
     * Enum to identify important properties of a {@link TestStep}.
     *
     * <p>Used in {@link TableTestProgramRunner#supportedSetupSteps()} and {@link
     * TableTestProgramRunner#supportedRunSteps()}.
     */
    enum TestKind {
        SQL,
        STATEMENT_SET,
        CONFIG,
        FUNCTION,
        SOURCE_WITHOUT_DATA,
        SOURCE_WITH_DATA,
        SOURCE_WITH_RESTORE_DATA,
        SINK_WITHOUT_DATA,
        SINK_WITH_DATA,
        SINK_WITH_RESTORE_DATA,
    }

    TestKind getKind();
}
