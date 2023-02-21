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

package org.apache.flink.runtime.webmonitor.handlers.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JarHandlerUtils}. */
class JarHandlerUtilsTest {

    private static final Logger LOG = LoggerFactory.getLogger(JarHandlerUtilsTest.class);

    @Test
    void testTokenizeNonQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--foo bar");
        assertThat(arguments.get(0)).isEqualTo("--foo");
        assertThat(arguments.get(1)).isEqualTo("bar");
    }

    @Test
    void testTokenizeSingleQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--foo 'bar baz '");
        assertThat(arguments.get(0)).isEqualTo("--foo");
        assertThat(arguments.get(1)).isEqualTo("bar baz ");
    }

    @Test
    void testTokenizeDoubleQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--name \"K. Bote \"");
        assertThat(arguments.get(0)).isEqualTo("--name");
        assertThat(arguments.get(1)).isEqualTo("K. Bote ");
    }

    @Test
    void testFromRequestDefaults(@TempDir Path tmp) throws RestHandlerException {
        final JarRunMessageParameters parameters =
                JarRunHeaders.getInstance().getUnresolvedMessageParameters();

        parameters.jarIdPathParameter.resolve("someJar");

        final HandlerRequest<JarPlanRequestBody> request =
                HandlerRequest.create(new JarPlanRequestBody(), parameters);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tmp, LOG);
        assertThat(jarHandlerContext.getEntryClass()).isNull();
        assertThat(jarHandlerContext.getProgramArgs()).isEmpty();
        assertThat(jarHandlerContext.getParallelism())
                .isEqualTo(CoreOptions.DEFAULT_PARALLELISM.defaultValue());
        assertThat(jarHandlerContext.getJobId()).isNull();
    }

    @Test
    void testFromRequestRequestBody(@TempDir Path tmp) throws RestHandlerException {
        final JarRunMessageParameters parameters =
                JarRunHeaders.getInstance().getUnresolvedMessageParameters();

        parameters.jarIdPathParameter.resolve("someJar");

        final JarPlanRequestBody requestBody =
                new JarPlanRequestBody(
                        "entry-class",
                        null,
                        Arrays.asList("arg1", "arg2"),
                        37,
                        JobID.generate(),
                        null);
        final HandlerRequest<JarPlanRequestBody> request =
                HandlerRequest.create(requestBody, parameters);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tmp, LOG);
        assertThat(jarHandlerContext.getEntryClass()).isEqualTo(requestBody.getEntryClassName());
        assertThat(jarHandlerContext.getProgramArgs())
                .containsExactlyElementsOf(requestBody.getProgramArgumentsList());
        assertThat(jarHandlerContext.getParallelism()).isEqualTo(requestBody.getParallelism());
        assertThat(jarHandlerContext.getJobId()).isEqualTo(requestBody.getJobId());
    }

    @Test
    void testFromRequestWithParallelismConfig(@TempDir Path tmp) throws RestHandlerException {
        final int parallelism = 37;
        final JarRunMessageParameters parameters =
                JarRunHeaders.getInstance().getUnresolvedMessageParameters();

        parameters.jarIdPathParameter.resolve("someJar");

        final JarPlanRequestBody requestBody =
                new JarPlanRequestBody(
                        "entry-class",
                        null,
                        Arrays.asList("arg1", "arg2"),
                        null,
                        JobID.generate(),
                        Collections.singletonMap(
                                CoreOptions.DEFAULT_PARALLELISM.key(),
                                String.valueOf(parallelism)));
        final HandlerRequest<JarPlanRequestBody> request =
                HandlerRequest.create(requestBody, parameters);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tmp, LOG);
        assertThat(jarHandlerContext.getParallelism()).isEqualTo(parallelism);
    }
}
