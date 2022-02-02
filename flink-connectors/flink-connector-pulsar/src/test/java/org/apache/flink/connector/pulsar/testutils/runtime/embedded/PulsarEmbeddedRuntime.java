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

package org.apache.flink.connector.pulsar.testutils.runtime.embedded;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.util.FileUtils;

import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.flink.connector.pulsar.testutils.runtime.embedded.PortBindingUtils.findAvailablePort;
import static org.apache.flink.connector.pulsar.testutils.runtime.embedded.PortBindingUtils.releasePorts;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.ServiceConfigurationUtils.brokerUrl;
import static org.apache.pulsar.broker.ServiceConfigurationUtils.webServiceUrl;

/** Providing a embedded pulsar server. We use this runtime for transaction related tests. */
public class PulsarEmbeddedRuntime implements PulsarRuntime {

    private static final String CONFIG_FILE_PATH;

    static {
        // Find the absolute path for containers/txnStandalone.conf
        ClassLoader classLoader = PulsarEmbeddedRuntime.class.getClassLoader();
        URL resource = classLoader.getResource("containers/txnStandalone.conf");
        File file = new File(checkNotNull(resource).getFile());
        CONFIG_FILE_PATH = file.getAbsolutePath();
    }

    private final int brokerServicePort;
    private final int webServicePort;
    private final Path tempDir;
    private final PulsarStandalone standalone;
    private PulsarRuntimeOperator operator;

    public PulsarEmbeddedRuntime() {
        ServiceConfiguration config;
        try (FileInputStream inputStream = new FileInputStream(CONFIG_FILE_PATH)) {
            config = PulsarConfigurationLoader.create(inputStream, ServiceConfiguration.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // Use runtime dynamic ports for broker.
        this.brokerServicePort = findAvailablePort();
        this.webServicePort = findAvailablePort();

        config.setAdvertisedAddress("localhost");
        config.setClusterName("standalone");

        config.setBrokerServicePort(Optional.of(brokerServicePort));
        config.setWebServicePort(Optional.of(webServicePort));

        // Select available port for bookkeeper and zookeeper.
        int zkPort = findAvailablePort();
        String zkConnect = "127.0.0.1" + ":" + zkPort;
        config.setZookeeperServers(zkConnect);
        config.setConfigurationStoreServers(zkConnect);
        config.setRunningStandalone(true);

        // ZkDir & BkDir
        this.tempDir = createTempDir();
        Path zkDir = Paths.get("data", "standalone", "zookeeper");
        Path bkDir = Paths.get("data", "standalone", "bookkeeper");

        this.standalone = new PulsarStandalone();

        standalone.setConfig(config);
        standalone.setConfigFile(CONFIG_FILE_PATH);
        standalone.setWipeData(true);
        standalone.setZkPort(zkPort);
        standalone.setBkPort(findAvailablePort());
        standalone.setZkDir(tempDir.resolve(zkDir).normalize().toString());
        standalone.setBkDir(tempDir.resolve(bkDir).normalize().toString());
        standalone.setNoFunctionsWorker(true);
        standalone.setNoStreamStorage(true);
        standalone.setAdvertisedAddress("localhost");
    }

    @Override
    public void startUp() {
        try {
            standalone.start();

            // Create the operator.
            String brokerUrl = brokerUrl("localhost", brokerServicePort);
            String webServiceUrl = webServiceUrl("localhost", webServicePort);
            this.operator = new PulsarRuntimeOperator(brokerUrl, webServiceUrl);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void tearDown() {
        try {
            if (operator != null) {
                operator.close();
                this.operator = null;
            }
            standalone.close();
            releasePorts();
            removeTempDir(tempDir);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public PulsarRuntimeOperator operator() {
        return checkNotNull(operator, "You should start this embedded Pulsar first.");
    }

    private Path createTempDir() {
        try {
            return Files.createTempDirectory("pulsar");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void removeTempDir(Path tempDir) {
        try {
            FileUtils.deleteDirectory(tempDir.normalize().toFile());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
