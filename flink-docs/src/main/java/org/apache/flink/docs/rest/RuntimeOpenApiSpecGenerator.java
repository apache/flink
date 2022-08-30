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

import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.util.DocumentingDispatcherRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.util.ConfigurationException;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.flink.docs.rest.OpenApiSpecGenerator.createDocumentationFile;

/**
 * OpenAPI spec generator for the Runtime Rest API.
 *
 * <p>One OpenAPI yml file is generated for each {@link RestServerEndpoint} implementation that can
 * be embedded into .md files using {@code {% include ${generated.docs.dir}/file.yml %}}.
 */
public class RuntimeOpenApiSpecGenerator {

    /**
     * Generates the Runtime REST API OpenAPI spec.
     *
     * @param args args[0] contains the directory into which the generated files are placed
     * @throws IOException if any file operation failed
     */
    public static void main(String[] args) throws IOException, ConfigurationException {
        String outputDirectory = args[0];

        for (final RuntimeRestAPIVersion apiVersion : RuntimeRestAPIVersion.values()) {
            if (apiVersion == RuntimeRestAPIVersion.V0) {
                // this version exists only for testing purposes
                continue;
            }
            createDocumentationFile(
                    new DocumentingDispatcherRestEndpoint(),
                    apiVersion,
                    Paths.get(
                            outputDirectory,
                            "rest_" + apiVersion.getURLVersionPrefix() + "_dispatcher.yml"));
        }
    }
}
