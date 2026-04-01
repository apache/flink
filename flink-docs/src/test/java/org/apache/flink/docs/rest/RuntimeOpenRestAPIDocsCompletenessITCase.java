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

import org.apache.flink.docs.util.Utils;
import org.apache.flink.runtime.rest.util.DocumentingDispatcherRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.flink.docs.rest.RuntimeOpenApiSpecGenerator.RUNTIME_OPEN_API_TITLE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that generated Runtime REST and Open API docs (HTML + OpenAPI yml) match committed
 * files.
 *
 * <p>This acts as a freshness check to ensure the documentation stays in sync with the code.
 */
class RuntimeOpenRestAPIDocsCompletenessITCase {

    @Test
    void testRuntimeRestApiDocsUpToDate(@TempDir Path tempDir) throws Exception {
        final DocumentingDispatcherRestEndpoint endpoint = new DocumentingDispatcherRestEndpoint();
        for (final RuntimeRestAPIVersion apiVersion : RuntimeRestAPIVersion.values()) {
            if (apiVersion == RuntimeRestAPIVersion.V0) {
                continue;
            }
            final String version = apiVersion.getURLVersionPrefix();
            String targetHtmlName = String.format("rest_%s_dispatcher.html", version);
            final Path pathOfGeneratedHtml = tempDir.resolve(targetHtmlName);
            final Path pathOfCommittedHtml = getPathOfCommittedHtml(targetHtmlName);
            RestAPIDocGenerator.createHtmlFile(endpoint, apiVersion, pathOfGeneratedHtml);
            assertThat(pathOfCommittedHtml)
                    .as("Missing committed %s file: %s", targetHtmlName, pathOfCommittedHtml)
                    .exists()
                    .as(
                            "Committed `%s` file is out of date. Please regenerate docs under `flink-docs` module based on `README.md`.",
                            targetHtmlName)
                    .hasSameTextualContentAs(pathOfGeneratedHtml);

            String targetYmlName = String.format("rest_%s_dispatcher.yml", version);
            final Path pathOfGeneratedYml = tempDir.resolve(targetYmlName);
            final Path pathOfCommittedYml = getPathOfCommittedYaml(targetYmlName);
            OpenApiSpecGenerator.createDocumentationFile(
                    RUNTIME_OPEN_API_TITLE, endpoint, apiVersion, pathOfGeneratedYml);
            assertThat(pathOfCommittedYml)
                    .as("Missing committed %s file: %s", targetYmlName, pathOfCommittedYml)
                    .exists()
                    .as(
                            "Committed `%s` file is out of date. Please regenerate docs under `flink-docs` module based on `README.md`.",
                            targetYmlName)
                    .hasSameTextualContentAs(pathOfGeneratedYml);
        }
    }

    static Path getPathOfCommittedHtml(String fileName) {
        final String rootDir = Utils.getProjectRootDir();
        return Paths.get(rootDir, "docs", "layouts", "shortcodes", "generated", fileName)
                .toAbsolutePath();
    }

    static Path getPathOfCommittedYaml(String fileName) {
        final String rootDir = Utils.getProjectRootDir();
        return Paths.get(rootDir, "docs", "static", "generated", fileName).toAbsolutePath();
    }
}
