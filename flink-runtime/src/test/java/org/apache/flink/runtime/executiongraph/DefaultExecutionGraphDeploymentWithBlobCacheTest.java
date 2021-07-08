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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.VoidBlobStore;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Tests {@link ExecutionGraph} deployment when offloading job and task information into the BLOB
 * server.
 */
public class DefaultExecutionGraphDeploymentWithBlobCacheTest
        extends DefaultExecutionGraphDeploymentWithBlobServerTest {

    @Before
    @Override
    public void setupBlobServer() throws IOException {
        Configuration config = new Configuration();
        // always offload the serialized job and task information
        config.setInteger(BlobServerOptions.OFFLOAD_MINSIZE, 0);
        blobServer = new BlobServer(config, new VoidBlobStore());
        blobServer.start();
        blobWriter = blobServer;

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());
        blobCache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress);
    }

    @After
    @Override
    public void shutdownBlobServer() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }
}
