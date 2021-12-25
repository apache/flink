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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A simple {@link org.junit.jupiter.api.extension.Extension} to be used by tests that require a
 * {@link BlobServer}.
 */
public class BlobServerExtension implements CustomExtension {
    private static final Logger LOG = LoggerFactory.getLogger(BlobServerExtension.class);
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private BlobServer blobServer;

    @Override
    public void before(ExtensionContext context) throws Exception {
        temporaryFolder.create();

        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

        blobServer = new BlobServer(config, new VoidBlobStore());
        blobServer.start();
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        temporaryFolder.delete();

        try {
            blobServer.close();
        } catch (IOException e) {
            LOG.error("Exception while shutting down blob server.", e);
        }
    }

    public int getBlobServerPort() {
        return blobServer.getPort();
    }

    public BlobServer getBlobServer() {
        return blobServer;
    }
}
