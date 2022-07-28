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

import java.io.IOException;

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
        config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, getConnectString());

        return ZooKeeperUtils.startCuratorFramework(config, fatalErrorHandler);
    }

    public void restart() throws Exception {
        getRunningZookeeperInstanceOrFail().restart();
    }

    public void stop() throws IOException {
        getRunningZookeeperInstanceOrFail().stop();
    }
}
