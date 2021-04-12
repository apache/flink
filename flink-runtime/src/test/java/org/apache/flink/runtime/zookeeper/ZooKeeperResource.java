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

import org.apache.flink.util.Preconditions;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link ExternalResource} which starts a {@link org.apache.zookeeper.server.ZooKeeperServer}. */
public class ZooKeeperResource extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperResource.class);

    @Nullable private TestingServer zooKeeperServer;

    public String getConnectString() {
        verifyIsRunning();
        return zooKeeperServer.getConnectString();
    }

    private void verifyIsRunning() {
        Preconditions.checkState(zooKeeperServer != null);
    }

    @Override
    protected void before() throws Throwable {
        terminateZooKeeperServer();
        zooKeeperServer = new TestingServer(true);
    }

    private void terminateZooKeeperServer() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.stop();
            zooKeeperServer = null;
        }
    }

    @Override
    protected void after() {
        try {
            terminateZooKeeperServer();
        } catch (IOException e) {
            LOG.warn("Could not properly terminate the {}.", getClass().getSimpleName(), e);
        }
    }

    public void restart() throws Exception {
        Preconditions.checkNotNull(zooKeeperServer);
        zooKeeperServer.restart();
    }
}
