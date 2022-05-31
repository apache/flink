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

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeUtils.initializePulsarEnvironment;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.broker.ServiceConfigurationUtils.brokerUrl;
import static org.apache.pulsar.broker.ServiceConfigurationUtils.webServiceUrl;

/** Providing a embedded pulsar server. We use this runtime for transaction related tests. */
public class PulsarEmbeddedRuntime implements PulsarRuntime {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarEmbeddedRuntime.class);

    private static final String CONFIG_FILE_PATH;

    static {
        // Find the absolute path for containers/txnStandalone.conf
        ClassLoader classLoader = PulsarEmbeddedRuntime.class.getClassLoader();
        URL resource = classLoader.getResource("containers/txnStandalone.conf");
        File file = new File(checkNotNull(resource).getFile());
        CONFIG_FILE_PATH = file.getAbsolutePath();
    }

    private final Path tempDir;

    private LocalBookkeeperEnsemble bookkeeper;
    private PulsarService pulsarService;
    private PulsarRuntimeOperator operator;

    public PulsarEmbeddedRuntime() {
        this.tempDir = createTempDir();
    }

    @Override
    public void startUp() {
        try {
            startBookkeeper();
            startPulsarService();

            // Create the operator.
            this.operator = new PulsarRuntimeOperator(serviceUrl(), adminUrl());
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
            if (pulsarService != null) {
                pulsarService.close();
            }
            if (bookkeeper != null) {
                bookkeeper.stop();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            removeTempDir(tempDir);
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

    public void startBookkeeper() throws Exception {
        Path zkPath = Paths.get("data", "standalone", "zookeeper");
        Path bkPath = Paths.get("data", "standalone", "bookkeeper");

        String zkDir = tempDir.resolve(zkPath).normalize().toString();
        String bkDir = tempDir.resolve(bkPath).normalize().toString();

        ServerConfiguration bkServerConf = new ServerConfiguration();
        bkServerConf.loadConf(new File(CONFIG_FILE_PATH).toURI().toURL());
        this.bookkeeper = new LocalBookkeeperEnsemble(1, 0, 0, zkDir, bkDir, true, "127.0.0.1");

        // Start Bookkeeper & zookeeper.
        bookkeeper.startStandalone(bkServerConf, false);
    }

    private void startPulsarService() throws Exception {
        ServiceConfiguration config;
        try (FileInputStream inputStream = new FileInputStream(CONFIG_FILE_PATH)) {
            config = PulsarConfigurationLoader.create(inputStream, ServiceConfiguration.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // Use runtime dynamic ports for broker.
        config.setAdvertisedAddress("127.0.0.1");
        config.setClusterName("standalone");

        // Use random port.
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));

        // Select available port for bookkeeper and zookeeper.
        int zkPort = getZkPort();
        String zkConnect = "127.0.0.1" + ":" + zkPort;
        config.setZookeeperServers(zkConnect);
        config.setConfigurationStoreServers(zkConnect);
        config.setRunningStandalone(true);

        this.pulsarService = new PulsarService(config);

        // Start Pulsar Broker.
        pulsarService.start();

        // Create sample data environment.
        initializePulsarEnvironment(config, serviceUrl(), adminUrl());
    }

    private int getZkPort() {
        return checkNotNull(bookkeeper).getZookeeperPort();
    }

    private String serviceUrl() {
        Integer port = pulsarService.getBrokerListenPort().orElseThrow(IllegalStateException::new);
        return brokerUrl("127.0.0.1", port);
    }

    private String adminUrl() {
        Integer port = pulsarService.getListenPortHTTP().orElseThrow(IllegalArgumentException::new);
        return webServiceUrl("127.0.0.1", port);
    }
}
