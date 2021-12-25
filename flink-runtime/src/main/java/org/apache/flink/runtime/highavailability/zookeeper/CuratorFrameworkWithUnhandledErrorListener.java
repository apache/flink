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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.UnhandledErrorListener;

import java.io.Closeable;

/**
 * A wrapper for curatorFramework and unHandledErrorListener which should be unregister from
 * curatorFramework before closing it.
 */
public class CuratorFrameworkWithUnhandledErrorListener implements Closeable {

    private final CuratorFramework client;

    private final UnhandledErrorListener listener;

    public CuratorFrameworkWithUnhandledErrorListener(
            CuratorFramework client, UnhandledErrorListener listener) {
        this.client = Preconditions.checkNotNull(client);
        this.listener = Preconditions.checkNotNull(listener);
    }

    @Override
    public void close() {
        client.getUnhandledErrorListenable().removeListener(listener);
        client.close();
    }

    public CuratorFramework asCuratorFramework() {
        return client;
    }
}
