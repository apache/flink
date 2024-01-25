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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link org.junit.jupiter.api.extension.Extension} which starts a {@link
 * org.apache.zookeeper.server.ZooKeeperServer}.
 */
public class ZooKeeperExtension implements CustomExtension {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperExtension.class);

    @Nullable private TestingServer zooKeeperServer;

    @Nullable private CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    @Override
    public void before(ExtensionContext context) throws Exception {
        close();
        zooKeeperServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        try {
            close();
        } catch (IOException e) {
            LOG.warn("Could not properly terminate the {}.", getClass().getSimpleName(), e);
        }
    }

    public void close() throws IOException {
        terminateCuratorFrameworkWrapper();
        terminateZooKeeperServer();
    }

    private void terminateCuratorFrameworkWrapper() {
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
            curatorFrameworkWrapper = null;
        }
    }

    private void terminateZooKeeperServer() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.close();
            zooKeeperServer = null;
        }
    }

    public String getConnectString() {
        return getRunningZookeeperInstanceOrFail().getConnectString();
    }

    private String[] getConnectionInfo() {
        final String connectStr = getConnectString();
        final String[] connectionInfo = connectStr.split(":");

        Preconditions.checkState(
                connectionInfo.length == 2,
                "The connect string doesn't match the expected format <host>:<port> (actual: %s)",
                connectStr);

        return connectionInfo;
    }

    public String getHost() {
        return getConnectionInfo()[0];
    }

    public int getPort() {
        return Integer.parseInt(getConnectionInfo()[1]);
    }

    public Iterable<String> executeCommand(String command) throws IOException {
        try (final Socket echoSocket = new Socket(getHost(), getPort())) {
            final PrintWriter out = new PrintWriter(echoSocket.getOutputStream(), true);
            final BufferedReader in =
                    new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));

            out.write(command);
            out.flush();

            String line = in.readLine();
            List<String> lines = new ArrayList<>();
            while (line != null) {
                lines.add(line);
                line = in.readLine();
            }

            return lines;
        }
    }

    public AutoCloseable whitelistAdminCommand(String command) {
        final String whitelistSystemProperty = "zookeeper.4lw.commands.whitelist";
        System.getProperties().setProperty(whitelistSystemProperty, command);

        return () -> System.getProperties().remove(whitelistSystemProperty);
    }

    private TestingServer getRunningZookeeperInstanceOrFail() {
        Preconditions.checkState(zooKeeperServer != null);
        return zooKeeperServer;
    }

    public CuratorFramework getZooKeeperClient(FatalErrorHandler fatalErrorHandler) {
        if (curatorFrameworkWrapper == null) {
            curatorFrameworkWrapper = createCuratorFramework(fatalErrorHandler);
        }

        return curatorFrameworkWrapper.asCuratorFramework();
    }

    private CuratorFrameworkWithUnhandledErrorListener createCuratorFramework(
            FatalErrorHandler fatalErrorHandler) {
        final Configuration config = new Configuration();
        config.set(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, getConnectString());

        return ZooKeeperUtils.startCuratorFramework(config, fatalErrorHandler);
    }

    public void restart() throws Exception {
        getRunningZookeeperInstanceOrFail().restart();
    }

    public void stop() throws IOException {
        getRunningZookeeperInstanceOrFail().stop();
    }
}
