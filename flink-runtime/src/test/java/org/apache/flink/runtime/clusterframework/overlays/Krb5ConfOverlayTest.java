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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.runtime.clusterframework.overlays.Krb5ConfOverlay.JAVA_SECURITY_KRB5_CONF;
import static org.apache.flink.runtime.clusterframework.overlays.Krb5ConfOverlay.TARGET_PATH;
import static org.junit.Assert.assertEquals;

/** Tests for {@link Krb5ConfOverlay}. */
public class Krb5ConfOverlayTest extends ContainerOverlayTestBase {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testConfigure() throws Exception {

        File krb5conf = tempFolder.newFile();

        Krb5ConfOverlay overlay = new Krb5ConfOverlay(krb5conf);

        ContainerSpecification spec = new ContainerSpecification();
        overlay.configure(spec);

        assertEquals(
                TARGET_PATH.getPath(),
                spec.getSystemProperties().getString(JAVA_SECURITY_KRB5_CONF, null));
        checkArtifact(spec, TARGET_PATH);
    }

    @Test
    public void testNoConf() throws Exception {
        Krb5ConfOverlay overlay = new Krb5ConfOverlay((Path) null);

        ContainerSpecification containerSpecification = new ContainerSpecification();
        overlay.configure(containerSpecification);
    }

    @Test
    public void testBuildOverlayFromConf() throws IOException {
        final File krb5conf = tempFolder.newFile();
        final Configuration configuration = new Configuration();
        configuration.set(SecurityOptions.KERBEROS_KRB5_PATH, krb5conf.getPath());
        final Krb5ConfOverlay overlay =
                Krb5ConfOverlay.newBuilder().fromEnvironmentOrConfiguration(configuration).build();
        assertEquals(overlay.krb5Conf.getPath(), krb5conf.getPath());
    }

    @Test
    public void testBuildOverlayFromEnv() throws IOException {
        final File krb5conf = tempFolder.newFile();
        final Configuration configuration = new Configuration();
        System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5conf.getPath());
        final Krb5ConfOverlay overlay =
                Krb5ConfOverlay.newBuilder().fromEnvironmentOrConfiguration(configuration).build();
        assertEquals(overlay.krb5Conf.getPath(), krb5conf.getPath());
    }
}
