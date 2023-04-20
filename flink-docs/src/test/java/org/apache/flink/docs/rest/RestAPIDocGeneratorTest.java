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

import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.docs.rest.data.TestEmptyMessageHeaders;
import org.apache.flink.docs.rest.data.TestExcludeMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RestAPIDocGenerator}. */
class RestAPIDocGeneratorTest {

    @Test
    void testExcludeFromDocumentation() throws Exception {
        File file = File.createTempFile("rest_v0_", ".html");
        RestAPIDocGenerator.createHtmlFile(
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
                file.toPath());
        String actual = FileUtils.readFile(file, "UTF-8");

        assertThat(actual).containsSequence("/test/empty1");
        assertThat(actual).containsSequence("This is a testing REST API.");
        assertThat(actual).containsSequence("/test/empty2");
        assertThat(actual).containsSequence("This is another testing REST API.");
        assertThat(actual).doesNotContain("/test/exclude1");
        assertThat(actual)
                .doesNotContain("This REST API should not appear in the generated documentation.");
        assertThat(actual).doesNotContain("/test/exclude2");
        assertThat(actual)
                .doesNotContain(
                        "This REST API should also not appear in the generated documentation.");
    }

    @Test
    void testAdditionalFields() {
        final String messageHtmlEntry =
                RestAPIDocGenerator.createMessageHtmlEntry(
                        TestAdditionalFields.class, null, EmptyRequestBody.class);
        assertThat(messageHtmlEntry)
                .isEqualTo(
                        "{\n"
                                + "  \"type\" : \"object\",\n"
                                + "  \"id\" : \"urn:jsonschema:org:apache:flink:docs:rest:RestAPIDocGeneratorTest:TestAdditionalFields\",\n"
                                + "  \"additionalProperties\" : {\n"
                                + "    \"type\" : \"string\"\n"
                                + "  }\n"
                                + "}");
    }

    @FlinkJsonSchema.AdditionalFields(type = String.class)
    private static class TestAdditionalFields {}
}
