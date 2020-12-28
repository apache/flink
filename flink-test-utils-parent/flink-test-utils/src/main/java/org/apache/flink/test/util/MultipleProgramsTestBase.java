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

package org.apache.flink.test.util;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Base class for unit tests that run multiple tests and want to reuse the same Flink cluster. This
 * saves a significant amount of time, since the startup and shutdown of the Flink clusters
 * (including actor systems, etc) usually dominates the execution of the actual tests.
 *
 * <p>To write a unit test against this test base, simply extend it and add one or more regular test
 * methods and retrieve the ExecutionEnvironment from the context:
 *
 * <pre>{@code
 * {@literal @}Test
 * public void someTest() {
 *     ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *     // test code
 *     env.execute();
 * }
 *
 * {@literal @}Test
 * public void anotherTest() {
 *     ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *     // test code
 *     env.execute();
 * }
 *
 * }</pre>
 */
public class MultipleProgramsTestBase extends AbstractTestBase {

    /**
     * Enum that defines which execution environment to run the next test on: An embedded local
     * flink cluster, or the collection execution backend.
     */
    public enum TestExecutionMode {
        CLUSTER,
        CLUSTER_OBJECT_REUSE,
        COLLECTION,
    }

    // ------------------------------------------------------------------------

    protected final TestExecutionMode mode;

    public MultipleProgramsTestBase(TestExecutionMode mode) {
        this.mode = mode;
    }

    // ------------------------------------------------------------------------
    //  Environment setup & teardown
    // ------------------------------------------------------------------------

    @Before
    public void setupEnvironment() {
        TestEnvironment testEnvironment;
        switch (mode) {
            case CLUSTER:
                // This only works because of the quirks we built in the TestEnvironment.
                // We should refactor this in the future!!!
                testEnvironment = miniClusterResource.getTestEnvironment();
                testEnvironment.getConfig().disableObjectReuse();
                testEnvironment.setAsContext();
                break;
            case CLUSTER_OBJECT_REUSE:
                // This only works because of the quirks we built in the TestEnvironment.
                // We should refactor this in the future!!!
                testEnvironment = miniClusterResource.getTestEnvironment();
                testEnvironment.getConfig().enableObjectReuse();
                testEnvironment.setAsContext();
                break;
            case COLLECTION:
                new CollectionTestEnvironment().setAsContext();
                break;
        }
    }

    @After
    public void teardownEnvironment() {
        switch (mode) {
            case CLUSTER:
            case CLUSTER_OBJECT_REUSE:
                TestEnvironment.unsetAsContext();
                break;
            case COLLECTION:
                CollectionTestEnvironment.unsetAsContext();
                break;
        }
    }

    // ------------------------------------------------------------------------
    //  Parametrization lets the tests run in cluster and collection mode
    // ------------------------------------------------------------------------

    @Parameterized.Parameters(name = "Execution mode = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {TestExecutionMode.CLUSTER},
                new Object[] {TestExecutionMode.COLLECTION});
    }
}
