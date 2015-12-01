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

package org.apache.flink.tez.test;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.tez.client.LocalTezEnvironment;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public abstract class TezProgramTestBase extends AbstractTestBase {

    private static final int DEFAULT_DEGREE_OF_PARALLELISM = 4;

    private JobExecutionResult latestExecutionResult;

    private int degreeOfParallelism = DEFAULT_DEGREE_OF_PARALLELISM;


    public TezProgramTestBase() {
        this(new Configuration());
    }

    public TezProgramTestBase(Configuration config) {
        super (config);
    }


    public void setParallelism(int degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
    }

    public JobExecutionResult getLatestExecutionResult() {
        return this.latestExecutionResult;
    }


    protected abstract void testProgram() throws Exception;

    protected void preSubmit() throws Exception {}

    protected void postSubmit() throws Exception {}

    // --------------------------------------------------------------------------------------------
    //  Test entry point
    // --------------------------------------------------------------------------------------------

    // Ignored due to deadlocks in Tez 0.6.1 (https://s3.amazonaws.com/archive.travis-ci.org/jobs/67848151/log.txt)
    // TODO Reactivate with future Tez versions
    @Ignore
    @Test
    public void testJob() throws Exception {
        // pre-submit
        try {
            preSubmit();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail("Pre-submit work caused an error: " + e.getMessage());
        }

        // prepare the test environment
        LocalTezEnvironment env = LocalTezEnvironment.create();
        env.setParallelism(degreeOfParallelism);
        env.setAsContext();

        // call the test program
        try {
            testProgram();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail("Error while calling the test program: " + e.getMessage());
        }

        // post-submit
        try {
            postSubmit();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            Assert.fail("Post-submit work caused an error: " + e.getMessage());
        }
    }


}
