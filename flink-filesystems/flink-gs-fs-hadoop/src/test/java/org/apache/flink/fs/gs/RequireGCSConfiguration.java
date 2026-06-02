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

package org.apache.flink.fs.gs;

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Extension run before GCS integration tests to check configuration assumptions. The GCS
 * integration tests require the GOOGLE_APPLICATION_CREDENTIALS environment variable, set to a path
 * on the local file system containing a JSON key file, and the GCS_BASE_PATH environment variable,
 * set to a gs:// path in a GCS bucket.
 */
public class RequireGCSConfiguration implements BeforeAllCallback {

    private static Path basePath;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        String credentialsEnv = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        assumeThat(credentialsEnv)
                .describedAs(
                        "GCS integration tests require GOOGLE_APPLICATION_CREDENTIALS "
                                + "environment variable with path to key file.")
                .isNotBlank();

        java.nio.file.Path credentialsFile = Paths.get(credentialsEnv);
        assertThat(credentialsFile)
                .describedAs(
                        String.format(
                                "GOOGLE_APPLICATION_CREDENTIALS environment variable is "
                                        + "set to a path (%s) that does not exist or is not readable.",
                                credentialsFile))
                .isReadable();

        assertThat(Files.isDirectory(credentialsFile))
                .describedAs(
                        String.format(
                                "GOOGLE_APPLICATION_CREDENTIALS environment variable is "
                                        + "set to a path (%s) that is a directory.",
                                credentialsFile))
                .isFalse();

        String basePathEnv = System.getenv("GCS_BASE_PATH");
        assumeThat(basePathEnv)
                .describedAs(
                        "GCS integration tests require GCS_BASE_PATH "
                                + "environment variable with gs:// path for storing test data.")
                .isNotBlank();

        assertThat(basePathEnv)
                .describedAs(
                        String.format(
                                "GCS_BASE_PATH environment variable is set to a path "
                                        + "(%s) on a different file system. The path must start with gs://.",
                                basePathEnv))
                .startsWith("gs://");

        basePath =
                new Path(
                        String.format(
                                "%s/%s-%s",
                                basePathEnv, context.getDisplayName(), UUID.randomUUID()));
    }

    public static Path getBasePath() {
        return basePath;
    }
}
