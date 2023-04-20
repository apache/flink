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

package org.apache.flink.docs.rest;

import org.apache.flink.docs.rest.data.TestAdditionalFieldsMessageHeaders;
import org.apache.flink.docs.rest.data.TestEmptyMessageHeaders;
import org.apache.flink.docs.rest.data.TestExcludeMessageHeaders;
import org.apache.flink.docs.rest.data.clash.inner.TestNameClashingMessageHeaders1;
import org.apache.flink.docs.rest.data.clash.inner.TestNameClashingMessageHeaders2;
import org.apache.flink.docs.rest.data.clash.top.pkg1.TestTopLevelNameClashingMessageHeaders1;
import org.apache.flink.docs.rest.data.clash.top.pkg2.TestTopLevelNameClashingMessageHeaders2;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.media.StringSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link OpenApiSpecGenerator}. */
class OpenApiSpecGeneratorTest {

    @Test
    void testTitle(@TempDir Path tmpDir) throws Exception {
        final String title = "Funky title";

        final Path file = tmpDir.resolve("openapi_spec.yaml");
        OpenApiSpecGenerator.createDocumentationFile(
                title,
                DocumentingRestEndpoint.forRestHandlerSpecifications(
                        new TestEmptyMessageHeaders("/test/empty1", "This is a testing REST API.")),
                RuntimeRestAPIVersion.V0,
                file);
        final String actual = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);

        assertThat(actual).contains("title: " + title);
    }

    @Test
    void testExcludeFromDocumentation(@TempDir Path tmpDir) throws Exception {
        final Path file = tmpDir.resolve("openapi_spec.yaml");
        OpenApiSpecGenerator.createDocumentationFile(
                "title",
                DocumentingRestEndpoint.forRestHandlerSpecifications(
                        new TestEmptyMessageHeaders("/test/empty1", "This is a testing REST API."),
                        new TestEmptyMessageHeaders(
                                "/test/empty2", "This is another testing REST API."),
                        new TestExcludeMessageHeaders(
                                "/test/exclude1",
                                "This REST API should not appear in the generated documentation."),
                        new TestExcludeMessageHeaders(
                                "/test/exclude2",
                                "This REST API should also not appear in the generated documentation.")),
                RuntimeRestAPIVersion.V0,
                file);
        final String actual = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);

        assertThat(actual).contains("/test/empty1");
        assertThat(actual).contains("This is a testing REST API.");
        assertThat(actual).contains("/test/empty2");
        assertThat(actual).contains("This is another testing REST API.");
        assertThat(actual).doesNotContain("/test/exclude1");
        assertThat(actual)
                .doesNotContain("This REST API should not appear in the generated documentation.");
        assertThat(actual).doesNotContain("/test/exclude2");
        assertThat(actual)
                .doesNotContain(
                        "This REST API should also not appear in the generated documentation.");
    }

    @Test
    void testDuplicateOperationIdsAreRejected(@TempDir Path tmpDir) {
        final Path file = tmpDir.resolve("openapi_spec.yaml");
        assertThatThrownBy(
                        () ->
                                OpenApiSpecGenerator.createDocumentationFile(
                                        "title",
                                        DocumentingRestEndpoint.forRestHandlerSpecifications(
                                                new TestEmptyMessageHeaders("operation1"),
                                                new TestEmptyMessageHeaders("operation1")),
                                        RuntimeRestAPIVersion.V0,
                                        file))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate OperationId");
    }

    @Test
    void testAdditionalFields(@TempDir Path tmpDir) throws Exception {
        final OpenAPI documentation =
                OpenApiSpecGenerator.createDocumentation(
                        "title",
                        DocumentingRestEndpoint.forRestHandlerSpecifications(
                                new TestAdditionalFieldsMessageHeaders("operation1")),
                        RuntimeRestAPIVersion.V0);
        assertThat(documentation.getComponents().getSchemas())
                .extractingByKey("AdditionalFieldsRequestBody")
                .satisfies(
                        x ->
                                assertThat(x.getAdditionalProperties())
                                        .isInstanceOf(StringSchema.class));
    }

    @Test
    void testModelNameClashByInnerClassesDetected() {
        assertThatThrownBy(
                        () ->
                                OpenApiSpecGenerator.createDocumentation(
                                        "title",
                                        DocumentingRestEndpoint.forRestHandlerSpecifications(
                                                new TestNameClashingMessageHeaders1(),
                                                new TestNameClashingMessageHeaders2()),
                                        RuntimeRestAPIVersion.V0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clash");
    }

    @Test
    void testModelNameClashByTopLevelClassesDetected() {
        assertThatThrownBy(
                        () ->
                                OpenApiSpecGenerator.createDocumentation(
                                        "title",
                                        DocumentingRestEndpoint.forRestHandlerSpecifications(
                                                new TestTopLevelNameClashingMessageHeaders1(),
                                                new TestTopLevelNameClashingMessageHeaders2()),
                                        RuntimeRestAPIVersion.V0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("clash");
    }
}
