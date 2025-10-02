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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test for SSL with certificate reload enabled. This test verifies that SSL-enabled
 * components can handle certificate changes without service disruption.
 */
public class SslWithReloadIT extends SslEndToEndITCaseBase {

    public SslWithReloadIT() throws IOException {
        super(true, true);
    }

    /**
     * Test SSL functionality with certificate reload enabled. Verifies that new certificates are
     * properly reloaded and used by the BlobServer.
     */
    @Test
    public void testSslBlobOperationsAndCertificateReload() throws Exception {
        LOG.info("Starting SSL end-to-end test: SSL enabled with reload");

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
            LOG.info("New certificates generated, waiting for reload...");

            // Wait for certificate reload on all ports
            final CertificateDates newCertDates =
                    getAllNewCertificateDates(ports, initialCertDates);

            // Verify all certificates were reloaded
            assertTrue(
                    "All certificates should be reloaded: " + newCertDates,
                    newCertDates.isAllPresent());

            // Verify certificate dates changed after reload
            assertNotEquals(
                    "BlobServer certificate notAfter date should change after reload",
                    initialCertDates.getBlobServerCertDate(),
                    newCertDates.getBlobServerCertDate());
            assertNotEquals(
                    "JobManager RPC certificate notAfter date should change after reload",
                    initialCertDates.getJobManagerRpcCertDate(),
                    newCertDates.getJobManagerRpcCertDate());
            assertNotEquals(
                    "Netty server certificate notAfter date should change after reload",
                    initialCertDates.getNettyServerCertDate(),
                    newCertDates.getNettyServerCertDate());
        }
    }
}
