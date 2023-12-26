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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtilsTest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** This class contains unit tests for the {@link BlobClient} with ssl enabled. */
class BlobClientSslTest extends BlobClientTest {

    /** The instance of the SSL BLOB server used during the tests. */
    private static BlobServer blobSslServer;

    /** Instance of a non-SSL BLOB server with SSL-enabled security options. */
    private static BlobServer blobNonSslServer;

    /** The SSL blob service client configuration. */
    private static Configuration sslClientConfig;

    /** The non-SSL blob service client configuration with SSL-enabled security options. */
    private static Configuration nonSslClientConfig;

    /** Starts the SSL enabled BLOB server. */
    @BeforeAll
    static void startSSLServer() throws IOException {
        Configuration config =
                SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores(
                        SecurityOptions.SSL_PROVIDER.defaultValue());

        blobSslServer = TestingBlobUtils.createServer(tempDir.resolve("ssl"), config);
        blobSslServer.start();

        sslClientConfig = config;
    }

    @BeforeAll
    static void startNonSSLServer() throws IOException {
        Configuration config =
                SSLUtilsTest.createInternalSslConfigWithKeyAndTrustStores(
                        SecurityOptions.SSL_PROVIDER.defaultValue());
        config.setBoolean(BlobServerOptions.SSL_ENABLED, false);

        blobNonSslServer = TestingBlobUtils.createServer(tempDir.resolve("non_ssl"), config);
        blobNonSslServer.start();

        nonSslClientConfig = config;
    }

    /** Shuts the BLOB server down. */
    @AfterAll
    static void stopServers() throws IOException {
        if (blobSslServer != null) {
            blobSslServer.close();
        }
        if (blobNonSslServer != null) {
            blobNonSslServer.close();
        }
    }

    @Override
    protected boolean isSSLEnabled() {
        return true;
    }

    protected Configuration getBlobClientConfig() {
        return sslClientConfig;
    }

    protected BlobServer getBlobServer() {
        return blobSslServer;
    }

    /** Verify ssl client to ssl server upload. */
    @Test
    public void testUploadJarFilesHelper() throws Exception {
        uploadJarFile(blobSslServer, sslClientConfig);
    }

    /** Verify ssl client to non-ssl server failure. */
    @Test
    public void testSSLClientFailure() {
        // SSL client connected to non-ssl server
        assertThatThrownBy(() -> uploadJarFile(blobServer, sslClientConfig))
                .isInstanceOf(IOException.class);
    }

    /** Verify ssl client to non-ssl server failure. */
    @Test
    public void testSSLClientFailure2() {
        // SSL client connected to non-ssl server
        assertThatThrownBy(() -> uploadJarFile(blobNonSslServer, sslClientConfig))
                .isInstanceOf(IOException.class);
    }

    /** Verify non-ssl client to ssl server failure. */
    @Test
    public void testSSLServerFailure() {
        // Non-SSL client connected to ssl server
        assertThatThrownBy(() -> uploadJarFile(blobSslServer, clientConfig))
                .isInstanceOf(IOException.class);
    }

    /** Verify non-ssl client to ssl server failure. */
    @Test
    public void testSSLServerFailure2() throws Exception {
        // Non-SSL client connected to ssl server
        assertThatThrownBy(() -> uploadJarFile(blobSslServer, nonSslClientConfig))
                .isInstanceOf(IOException.class);
    }

    /** Verify non-ssl connection sanity. */
    @Test
    public void testNonSSLConnection() throws Exception {
        uploadJarFile(blobServer, clientConfig);
    }

    /** Verify non-ssl connection sanity. */
    @Test
    public void testNonSSLConnection2() throws Exception {
        uploadJarFile(blobServer, nonSslClientConfig);
    }

    /** Verify non-ssl connection sanity. */
    @Test
    public void testNonSSLConnection3() throws Exception {
        uploadJarFile(blobNonSslServer, clientConfig);
    }

    /** Verify non-ssl connection sanity. */
    @Test
    public void testNonSSLConnection4() throws Exception {
        uploadJarFile(blobNonSslServer, nonSslClientConfig);
    }
}
