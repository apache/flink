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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.flink.runtime.clusterframework.overlays.SSLStoreOverlay.TARGET_KEYSTORE_PATH;
import static org.apache.flink.runtime.clusterframework.overlays.SSLStoreOverlay.TARGET_TRUSTSTORE_PATH;
import static org.junit.Assert.assertEquals;

public class SSLStoreOverlayTest extends ContainerOverlayTestBase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testConfigure() throws Exception {

        File keystore = tempFolder.newFile();
        File truststore = tempFolder.newFile();
        SSLStoreOverlay overlay = new SSLStoreOverlay(keystore, truststore);

        ContainerSpecification spec = new ContainerSpecification();
        overlay.configure(spec);

        assertEquals(
                TARGET_KEYSTORE_PATH.getPath(),
                spec.getFlinkConfiguration().getString(SecurityOptions.SSL_KEYSTORE));
        checkArtifact(spec, TARGET_KEYSTORE_PATH);

        assertEquals(
                TARGET_TRUSTSTORE_PATH.getPath(),
                spec.getFlinkConfiguration().getString(SecurityOptions.SSL_TRUSTSTORE));
        checkArtifact(spec, TARGET_TRUSTSTORE_PATH);
    }

    @Test
    public void testNoConf() throws Exception {
        SSLStoreOverlay overlay = new SSLStoreOverlay(null, null);

        ContainerSpecification containerSpecification = new ContainerSpecification();
        overlay.configure(containerSpecification);
    }

    @Test
    public void testBuilderFromEnvironment() throws Exception {

        final Configuration conf = new Configuration();
        File keystore = tempFolder.newFile();
        File truststore = tempFolder.newFile();

        conf.setString(SecurityOptions.SSL_KEYSTORE, keystore.getAbsolutePath());
        conf.setString(SecurityOptions.SSL_TRUSTSTORE, truststore.getAbsolutePath());

        SSLStoreOverlay.Builder builder = SSLStoreOverlay.newBuilder().fromEnvironment(conf);
        assertEquals(builder.keystorePath, keystore);
        assertEquals(builder.truststorePath, truststore);
    }
}
