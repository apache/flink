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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.net.SSLUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing a {@link BlobServer} would fail with improper SSL config. */
class BlobServerSslTest {

    @Test
    void testFailedToInitWithTwoProtocolsSet() {
        final Configuration config = new Configuration();

        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        config.set(
                SecurityOptions.SSL_KEYSTORE,
                getClass().getResource("/local127.keystore").getPath());
        config.set(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_KEY_PASSWORD, "password");
        config.set(
                SecurityOptions.SSL_TRUSTSTORE,
                getClass().getResource("/local127.truststore").getPath());

        config.set(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_ALGORITHMS, "TLSv1,TLSv1.1");

        assertThatThrownBy(() -> new BlobServer(config, new File("foobar"), new VoidBlobStore()))
                .isInstanceOf(IOException.class)
                .hasMessage("Unable to open BLOB Server in specified port range: 0");
    }

    @Test
    void testFailedToInitWithInvalidSslKeystoreConfigured() {
        final Configuration config = new Configuration();

        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        config.set(SecurityOptions.SSL_KEYSTORE, "invalid.keystore");
        config.set(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_KEY_PASSWORD, "password");
        config.set(SecurityOptions.SSL_TRUSTSTORE, "invalid.keystore");
        config.set(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");

        assertThatThrownBy(() -> new BlobServer(config, new File("foobar"), new VoidBlobStore()))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to initialize SSL for the blob server");
    }

    @Test
    void testFailedToInitWithMissingMandatorySslConfiguration() {
        final Configuration config = new Configuration();

        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);

        assertThatThrownBy(() -> new BlobServer(config, new File("foobar"), new VoidBlobStore()))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to initialize SSL for the blob server");
    }

    @Test
    void testInitWithSsl() throws Exception {
        final Configuration config = new Configuration();

        String tmpdir = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
        String tmpKeyStorePath = Paths.get(tmpdir, "local127.keystore").toString();
        String tmpTrustStorePath = Paths.get(tmpdir, "local127.truststore").toString();

        Files.copy(
                Paths.get(
                        BlobServerSslTest.class
                                .getResource("/local127.keystore")
                                .getFile()
                                .toString()),
                Paths.get(tmpKeyStorePath),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        BlobServerSslTest.class
                                .getResource("/local127.truststore")
                                .getFile()
                                .toString()),
                Paths.get(tmpTrustStorePath),
                StandardCopyOption.REPLACE_EXISTING);

        config.set(JobManagerOptions.BIND_HOST, "localhost");
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE, tmpKeyStorePath);
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, "password");
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE, tmpTrustStorePath);
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, "password");
        config.set(SecurityOptions.SSL_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA");

        BlobServer blobServer = new BlobServer(config, new File("foobar"), new VoidBlobStore());
        blobServer.start();

        CountDownLatch watchCertificate = new CountDownLatch(1);
        testLoadedCertificate(
                config,
                blobServer,
                "Validity: [From: Tue Feb 26 11:58:09 CET 2019",
                watchCertificate);
        Assertions.assertTrue(watchCertificate.await(10, TimeUnit.SECONDS));

        Files.copy(
                Paths.get(
                        BlobServerSslTest.class
                                .getResource("/local127.2.keystore")
                                .getFile()
                                .toString()),
                Paths.get(tmpKeyStorePath),
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
                Paths.get(
                        BlobServerSslTest.class
                                .getResource("/local127.2.truststore")
                                .getFile()
                                .toString()),
                Paths.get(tmpTrustStorePath),
                StandardCopyOption.REPLACE_EXISTING);

        watchCertificate = new CountDownLatch(1);
        testLoadedCertificate(
                config,
                blobServer,
                "Validity: [From: Tue Mar 18 15:08:24 CET 2025",
                watchCertificate);
        Assertions.assertTrue(watchCertificate.await(10, TimeUnit.SECONDS));

        blobServer.close();
    }

    void testLoadedCertificate(
            Configuration config,
            BlobServer blobServer,
            String validity,
            CountDownLatch watchCertificate)
            throws Exception {
        while (true) {
            SocketFactory clientSocketFactory = SSLUtils.createSSLClientSocketFactory(config);
            SSLSocket socket = null;
            try {
                socket =
                        (SSLSocket)
                                clientSocketFactory.createSocket(
                                        "localhost", blobServer.getServerSocket().getLocalPort());
                socket.startHandshake();
            } catch (IOException ignored) {
                continue;
            }
            if (!socket.getSession().isValid()) {
                continue;
            }
            if (!socket.getSession().getPeerCertificates()[0].toString().contains(validity)) {
                continue;
            }
            socket.close();
            watchCertificate.countDown();
            break;
        }
    }
}
