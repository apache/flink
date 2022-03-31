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

package org.apache.flink.client.testjar;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.util.FlinkException;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code ErrorHandlingSubmissionJob} provides a factory method for creating a {@link
 * PackagedProgram} that monitors the job submission within the job's {@code main} method.
 */
public class ErrorHandlingSubmissionJob {

    private static final AtomicReference<Exception> SUBMISSION_EXCEPTION = new AtomicReference<>();

    public static PackagedProgram createPackagedProgram() throws FlinkException {
        try {
            return PackagedProgram.newBuilder()
                    .setUserClassPaths(
                            Collections.singletonList(
                                    new File(CliFrontendTestUtils.getTestJarPath())
                                            .toURI()
                                            .toURL()))
                    .setEntryPointClassName(ErrorHandlingSubmissionJob.class.getName())
                    .build();
        } catch (ProgramInvocationException | FileNotFoundException | MalformedURLException e) {
            throw new FlinkException("Could not load the provided entrypoint class.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(Arrays.asList(1, 2, 3))
                .map(element -> element + 1)
                .output(new DiscardingOutputFormat<>());

        try {
            env.execute();
        } catch (Exception e) {
            SUBMISSION_EXCEPTION.set(e);
            throw e;
        }
    }

    public static Exception getSubmissionException() {
        return SUBMISSION_EXCEPTION.get();
    }
}
