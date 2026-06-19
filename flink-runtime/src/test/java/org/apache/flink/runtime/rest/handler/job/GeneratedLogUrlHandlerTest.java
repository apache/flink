/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link GeneratedLogUrlHandler}. */
class GeneratedLogUrlHandlerTest {

    @Test
    void testGenerateJobManagerLogUrl() {
        final String pattern = "http://localhost/<jobid>/log";
        final String jobId = "jobid";

        final String generatedUrl = GeneratedLogUrlHandler.generateLogUrl(pattern, jobId, null);

        assertThat(generatedUrl).isEqualTo(pattern.replace("<jobid>", jobId));
    }

    @Test
    void testGenerateTaskManagerLogUrl() {
        final String pattern = "http://localhost/<jobid>/tm/<tmid>/log";
        final String jobId = "jobid";
        final String taskManagerId = "tmid";

        final String generatedUrl =
                GeneratedLogUrlHandler.generateLogUrl(pattern, jobId, taskManagerId);

        assertThat(pattern.replace("<jobid>", jobId).replace("<tmid>", taskManagerId))
                .isEqualTo(generatedUrl);
    }
}
