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

package org.apache.flink.metrics.otel;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

/** {@link OtelTestContainer} provides an {@code Otel} test instance. */
class OtelTestContainer extends GenericContainer<OtelTestContainer> {
    private static final int DEFAULT_HTTP_PORT = 4318;
    private static final int DEFAULT_GRPC_PORT = 4317;

    // must be kept in sync with otel-config.yaml
    private static final String DATA_DIR = "/data";
    // must be kept in sync with otel-config.yaml
    private static final String LOG_FILE = "logs.json";
    private static final Path CONFIG_PATH =
            Paths.get("src/test/resources/").resolve("otel-config.yaml");

    public OtelTestContainer(Path outputDir) {
        super(DockerImageName.parse("otel/opentelemetry-collector:0.111.0"));
        withNetworkAliases(randomString("otel-collector", 6));
        addExposedPort(DEFAULT_HTTP_PORT);
        addExposedPort(DEFAULT_GRPC_PORT);
        withCopyFileToContainer(
                MountableFile.forHostPath(CONFIG_PATH.toString()), "otel-config.yaml");
        withCopyFileToContainer(
                MountableFile.forHostPath(
                        new File(outputDir.toFile(), LOG_FILE).getAbsolutePath(), 755),
                getOutputLogPath().toString());
        withCommand("--config", "otel-config.yaml");
    }

    public Path getOutputLogPath() {
        return Path.of(DATA_DIR, LOG_FILE);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    public String getHttpEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_HTTP_PORT));
    }

    public String getGrpcEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_GRPC_PORT));
    }
}
