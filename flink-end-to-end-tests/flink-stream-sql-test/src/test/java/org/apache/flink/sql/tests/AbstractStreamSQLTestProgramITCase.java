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

package org.apache.flink.sql.tests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.JobSubmission;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.xml.bind.DatatypeConverter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
abstract class AbstractStreamSQLTestProgramITCase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractStreamSQLTestProgramITCase.class);
    public static final Network NETWORK = Network.newNetwork();
    private static final Path STREAM_SQL_PROGRAM =
            ResourceTestUtils.getResource(".*/StreamSQLTestProgram\\.jar");

    FlinkContainers flink = createFlinkContainers();

    protected abstract String getPlannerJarName();

    protected abstract FlinkContainers createFlinkContainers();

    @BeforeEach
    void beforeEach() throws Exception {
        LOGGER.info("Starting Flink containers.");
        flink.start();
    }

    @AfterEach
    void afterEach() {
        LOGGER.info("Stopping Flink containers.");
        flink.stop();
    }

    @Test
    void testStreamSQL() throws Exception {
        String outputPath = "/tmp/stream-sql/records.out";
        final JobSubmission job =
                new JobSubmission.JobSubmissionBuilder(STREAM_SQL_PROGRAM) //
                        .setDetached(false) //
                        .addArgument("-p", "4") //
                        .addArgument("--outputPath", outputPath) //
                        .build();
        final JobID jobId = flink.submitJob(job);
        final String content = flink.getOutputPathContent(outputPath + "/20/", "part-*", true);
        assertThat(content)
                .isEqualTo(
                        "+I[20, 1970-01-01 00:00:00.0]\n" //
                                + "+I[20, 1970-01-01 00:00:20.0]\n" //
                                + "+I[20, 1970-01-01 00:00:40.0]\n" //
                        );
        assertThat(getMD5Hash(content)).isEqualTo("a88cc1dc7e7c2c2adc75bd23454ef4da");

        // Check uses correct planner jar
        Container.ExecResult result =
                flink.getJobManager()
                        .execInContainer("bash", "-c", "cat log/flink--standalone*.log");
        assertThat(result.getExitCode()).isZero();
        assertThat(result.getStdout()).contains(getPlannerJarName());
    }

    private String getMD5Hash(final String input) throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return DatatypeConverter.printHexBinary(bytes).toLowerCase();
    }
}
