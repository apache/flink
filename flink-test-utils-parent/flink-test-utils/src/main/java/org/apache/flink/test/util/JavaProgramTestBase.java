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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestStreamEnvironment;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

/**
 * Base class for unit tests that run a single test with object reuse enabled/disabled and against
 * collection environments.
 *
 * <p>To write a unit test against this test base, simply extend it and implement the {@link
 * #testProgram()} method.
 *
 * <p>To skip the execution against collection environments you have to override {@link
 * #skipCollectionExecution()}.
 */
public abstract class JavaProgramTestBase extends AbstractTestBase {

    private JobExecutionResult latestExecutionResult;

    /**
     * The number of times a test should be repeated.
     *
     * <p>This is useful for runtime changes, which affect resource management. Running certain
     * tests repeatedly might help to discover resource leaks, race conditions etc.
     */
    private int numberOfTestRepetitions = 1;

    private boolean isCollectionExecution;

    public void setNumberOfTestRepetitions(int numberOfTestRepetitions) {
        this.numberOfTestRepetitions = numberOfTestRepetitions;
    }

    public int getParallelism() {
        return isCollectionExecution ? 1 : MINI_CLUSTER_EXTENSION.getNumberSlots();
    }

    public JobExecutionResult getLatestExecutionResult() {
        return this.latestExecutionResult;
    }

    public boolean isCollectionExecution() {
        return isCollectionExecution;
    }

    // --------------------------------------------------------------------------------------------
    //  Methods to create the test program and for pre- and post- test work
    // --------------------------------------------------------------------------------------------

    protected abstract JobExecutionResult testProgram() throws Exception;

    protected void preSubmit() throws Exception {}

    protected void postSubmit() throws Exception {}

    protected boolean skipCollectionExecution() {
        return false;
    }

    // --------------------------------------------------------------------------------------------
    //  Test entry point
    // --------------------------------------------------------------------------------------------

    @Test
    public void testJobWithObjectReuse() throws Exception {
        isCollectionExecution = false;

        preSubmit();

        // This only works because the underlying ExecutionEnvironment is a TestEnvironment
        // We should fix that we are able to get access to the latest execution result from a
        // different
        // execution environment and how the object reuse mode is enabled
        TestStreamEnvironment env = MINI_CLUSTER_EXTENSION.getTestStreamEnvironment();
        env.getConfig().enableObjectReuse();

        // Possibly run the test multiple times
        executeProgramMultipleTimes(env);
    }

    private void executeProgramMultipleTimes(StreamExecutionEnvironment env) throws Exception {
        for (int i = 0; i < numberOfTestRepetitions; i++) {
            try {
                this.latestExecutionResult = testProgram();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                fail("Error while calling the test program: " + e.getMessage());
            }

            assertThat(this.latestExecutionResult)
                    .as("The test program never triggered an execution.")
                    .isNotNull();
        }

        postSubmit();
    }

    @Test
    public void testJobWithoutObjectReuse() throws Exception {
        isCollectionExecution = false;
        preSubmit();

        // This only works because the underlying ExecutionEnvironment is a TestEnvironment
        // We should fix that we are able to get access to the latest execution result from a
        // different
        // execution environment and how the object reuse mode is enabled
        StreamExecutionEnvironment env = MINI_CLUSTER_EXTENSION.getTestStreamEnvironment();
        env.getConfig().disableObjectReuse();

        // Possibly run the test multiple times
        executeProgramMultipleTimes(env);
    }
}
