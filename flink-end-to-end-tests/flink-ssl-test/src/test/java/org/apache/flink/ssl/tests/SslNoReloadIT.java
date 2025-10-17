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

package org.apache.flink.ssl.tests;

import org.apache.flink.tests.util.flink.ClusterController;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.attribute.FileTime;

import static org.junit.Assert.assertTrue;

/**
 * End-to-end test for SSL enabled without certificate reload. This test verifies that when SSL
 * reload is disabled, certificates are not reloaded even when they change on disk.
 */
public class SslNoReloadIT extends SslEndToEndITCaseBase {

    public SslNoReloadIT() throws IOException {
        super(true, false);
    }

    /**
     * Test SSL functionality without certificate reload. Verifies that certificates are NOT
     * reloaded and certificate files are NOT accessed when reload is disabled.
     */
    @Test
    public void testSslBlobOperationsWithoutCertificateReload() throws Exception {
        LOG.info("Starting SSL end-to-end test: SSL enabled without reload");

        // Start Flink cluster with the SSL configuration set in constructor
        try (ClusterController ignored = flinkResource.startCluster(1)) {
            final FlinkPorts ports = getAllPorts();

            // Verify all certificates are accessible
            final CertificateDates initialCertDates = getAllCertificateDates(ports);
            assertTrue(
                    "All certificates should be accessible: " + initialCertDates,
                    initialCertDates.isAllPresent());

            LOG.info("Generating new SSL certificates with {}-day validity", NEW_VALIDITY_DAYS);
            SslTestUtils.generateAndInstallCertificates(
                    internalSslDir, SSL_PASSWORD, NEW_VALIDITY_DAYS);
            FileTime keystoreAccessTimeBefore =
                    getFileAccessTime(internalSslDir.resolve(KEYSTORE_FILENAME));
            FileTime truststoreAccessTimeBefore =
                    getFileAccessTime(internalSslDir.resolve(TRUSTSTORE_FILENAME));

            verifyCertFilesAreNotAccessed(keystoreAccessTimeBefore, truststoreAccessTimeBefore);
        }
    }
}
