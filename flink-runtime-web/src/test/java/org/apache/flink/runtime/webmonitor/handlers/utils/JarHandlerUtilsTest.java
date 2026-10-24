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
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.webmonitor.handlers.JarPlanRequestBody;
import org.apache.flink.runtime.webmonitor.handlers.JarRunHeaders;
import org.apache.flink.runtime.webmonitor.handlers.JarRunMessageParameters;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JarHandlerUtils}. */
class JarHandlerUtilsTest {

    private static final Logger LOG = LoggerFactory.getLogger(JarHandlerUtilsTest.class);

    @TempDir private Path tempDir;

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
    void testFromRequestDefaults() throws Exception {
        final JarRunMessageParameters parameters = getDummyMessageParameters();

        final HandlerRequest<JarPlanRequestBody> request =
                HandlerRequest.create(new JarPlanRequestBody(), parameters);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);
        assertThat(jarHandlerContext.getEntryClass()).isNull();
        assertThat(jarHandlerContext.getProgramArgs()).isEmpty();
        assertThat(jarHandlerContext.getParallelism())
                .isEqualTo(CoreOptions.DEFAULT_PARALLELISM.defaultValue());
        assertThat(jarHandlerContext.getJobId()).isNull();
    }

    @Test
    void testFromRequestRequestBody() throws Exception {
        final JarPlanRequestBody requestBody = getDummyJarPlanRequestBody("entry-class", 37, null);
        final HandlerRequest<JarPlanRequestBody> request = getDummyRequest(requestBody);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);
        assertThat(jarHandlerContext.getEntryClass()).isEqualTo(requestBody.getEntryClassName());
        assertThat(jarHandlerContext.getProgramArgs())
                .containsExactlyElementsOf(requestBody.getProgramArgumentsList());
        assertThat(jarHandlerContext.getParallelism()).isEqualTo(requestBody.getParallelism());
        assertThat(jarHandlerContext.getJobId()).isEqualTo(requestBody.getJobId());
    }

    @Test
    void testFromRequestWithParallelismConfig() throws Exception {
        final int parallelism = 37;
        final JarPlanRequestBody requestBody =
                getDummyJarPlanRequestBody(
                        "entry-class",
                        null,
                        Collections.singletonMap(
                                CoreOptions.DEFAULT_PARALLELISM.key(),
                                String.valueOf(parallelism)));
        final HandlerRequest<JarPlanRequestBody> request = getDummyRequest(requestBody);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);
        assertThat(jarHandlerContext.getParallelism()).isEqualTo(parallelism);
    }

    @Test
    void testApplyingRequestConfigurationDoesNotOpenJar() throws Exception {
        final JarPlanRequestBody requestBody = getDummyJarPlanRequestBody("entry-class", 3, null);
        final HandlerRequest<JarPlanRequestBody> request = getDummyRequest(requestBody);
        // "someJar" does not exist, so building a PackagedProgram here would fail
        final JarHandlerUtils.JarHandlerContext context =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);
        final Configuration configuration = new Configuration();

        context.applyToConfiguration(configuration, request);

        assertThat(configuration.get(CoreOptions.DEFAULT_PARALLELISM)).isEqualTo(3);
        assertThat(configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID))
                .isEqualTo(requestBody.getJobId().toHexString());
        assertThat(configuration.contains(PipelineOptions.JARS)).isFalse();
        assertThat(configuration.contains(PipelineOptions.CLASSPATHS)).isFalse();
    }

    @Test
    void testApplyingProgramToConfigurationSetsJarsAndClasspaths() throws Exception {
        final Path jar = Files.createFile(tempDir.resolve("someJar"));
        try (JarOutputStream ignored = new JarOutputStream(Files.newOutputStream(jar))) {
            // an empty jar is sufficient
        }
        final URL classpath = new URL("file:/tmp/some.jar");
        final Configuration programConfiguration = new Configuration();
        programConfiguration.set(
                PipelineOptions.CLASSPATHS, Collections.singletonList(classpath.toString()));

        final HandlerRequest<JarPlanRequestBody> request =
                getDummyRequest(getDummyJarPlanRequestBody(EntryPoint.class.getName(), null, null));
        final JarHandlerUtils.JarHandlerContext context =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);

        final Configuration configuration = new Configuration();
        try (PackagedProgram program = context.toPackagedProgram(programConfiguration)) {
            context.applyProgramToConfiguration(configuration, program);

            assertThat(configuration.get(PipelineOptions.JARS))
                    .containsExactly(jar.toUri().toURL().toString());
            assertThat(configuration.get(PipelineOptions.CLASSPATHS))
                    .containsExactly(classpath.toString());
        }
    }

    @Test
    void testClasspathsConfigNotErased() throws Exception {
        final JarPlanRequestBody requestBody =
                getDummyJarPlanRequestBody(
                        null,
                        null,
                        Collections.singletonMap(
                                PipelineOptions.CLASSPATHS.key(),
                                "file:/tmp/some.jar;file:/tmp/another.jar"));

        final HandlerRequest<JarPlanRequestBody> request = getDummyRequest(requestBody);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);

        final Configuration originalConfig = request.getRequestBody().getFlinkConfiguration();
        final PackagedProgram.Builder builder =
                jarHandlerContext.initPackagedProgramBuilder(originalConfig);
        final List<String> retrievedClasspaths =
                builder.getUserClassPaths().stream()
                        .map(URL::toString)
                        .collect(Collectors.toList());

        assertThat(retrievedClasspaths).isEqualTo(originalConfig.get(PipelineOptions.CLASSPATHS));
    }

    @Test
    void testMalformedClasspathsConfig() throws Exception {
        final JarPlanRequestBody requestBody =
                getDummyJarPlanRequestBody(
                        null,
                        null,
                        Collections.singletonMap(
                                PipelineOptions.CLASSPATHS.key(), "invalid|:/jar"));
        final HandlerRequest<JarPlanRequestBody> request = getDummyRequest(requestBody);

        final JarHandlerUtils.JarHandlerContext jarHandlerContext =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);

        final Configuration originalConfig = request.getRequestBody().getFlinkConfiguration();

        assertThatThrownBy(() -> jarHandlerContext.initPackagedProgramBuilder(originalConfig))
                .satisfies(anyCauseMatches(RestHandlerException.class, "invalid|:/jar"));
    }

    @Test
    void testExtractedNestedLibrariesLiveUntilProgramIsClosed() throws Exception {
        final Path jar = tempDir.resolve("someJar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            out.putNextEntry(new JarEntry("lib/nested.jar"));
            out.write(new byte[] {1, 2, 3});
            out.closeEntry();
        }
        final HandlerRequest<JarPlanRequestBody> request =
                getDummyRequest(getDummyJarPlanRequestBody(EntryPoint.class.getName(), null, null));
        final JarHandlerUtils.JarHandlerContext context =
                JarHandlerUtils.JarHandlerContext.fromRequest(request, tempDir, LOG);

        final Configuration configuration = new Configuration();
        final PackagedProgram program = context.toPackagedProgram(new Configuration());
        context.applyProgramToConfiguration(configuration, program);

        final List<Path> jars =
                configuration.get(PipelineOptions.JARS).stream()
                        .map(url -> Path.of(URI.create(url)))
                        .collect(Collectors.toList());
        assertThat(jars).hasSize(2);
        assertThat(jars).allSatisfy(path -> assertThat(path).exists());

        program.close();

        assertThat(jars.get(0)).exists();
        assertThat(jars.get(1)).doesNotExist();
    }

    /** Program entry point used to build a {@link PackagedProgram}. */
    public static class EntryPoint {
        public static void main(String[] args) {}
    }

    private HandlerRequest<JarPlanRequestBody> getDummyRequest(
            @Nullable JarPlanRequestBody requestBody) {
        return HandlerRequest.create(
                requestBody == null ? new JarPlanRequestBody() : requestBody,
                getDummyMessageParameters());
    }

    private JarRunMessageParameters getDummyMessageParameters() {
        final JarRunMessageParameters parameters =
                JarRunHeaders.getInstance().getUnresolvedMessageParameters();

        parameters.jarIdPathParameter.resolve("someJar");

        return parameters;
    }

    private JarPlanRequestBody getDummyJarPlanRequestBody(
            String entryClass, Integer parallelism, Map<String, String> flinkConfiguration) {
        return new JarPlanRequestBody(
                entryClass,
                Arrays.asList("arg1", "arg2"),
                parallelism,
                JobID.generate(),
                flinkConfiguration);
    }
}
