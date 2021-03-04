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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesPodTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParametersTest;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/** General tests for the {@link KerberosMountDecoratorTest}. */
public class KerberosMountDecoratorTest extends KubernetesPodTestBase {

    private KerberosMountDecorator kerberosMountDecorator;

    private AbstractKubernetesParametersTest.TestingKubernetesParameters
            testingKubernetesParameters;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, kerberosDir.toString() + "/" + KEYTAB_FILE);
        flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test");
        flinkConfig.set(
                SecurityOptions.KERBEROS_KRB5_PATH, kerberosDir.toString() + "/" + KRB5_CONF_FILE);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        generateKerberosFileItems();

        this.testingKubernetesParameters =
                new AbstractKubernetesParametersTest.TestingKubernetesParameters(flinkConfig);
        this.kerberosMountDecorator = new KerberosMountDecorator(testingKubernetesParameters);
    }

    @Test
    public void testWhetherPodOrContainerIsDecorated() {
        final FlinkPod resultFlinkPod = kerberosMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertNotEquals(
                baseFlinkPod.getPodWithoutMainContainer(),
                resultFlinkPod.getPodWithoutMainContainer());
        assertNotEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
    }

    @Test
    public void testConfEditWhenBuildAccompanyingKubernetesResources() throws IOException {
        kerberosMountDecorator.buildAccompanyingKubernetesResources();

        assertEquals(
                String.format("%s/%s", Constants.KERBEROS_KEYTAB_MOUNT_POINT, KEYTAB_FILE),
                this.testingKubernetesParameters
                        .getFlinkConfiguration()
                        .get(SecurityOptions.KERBEROS_LOGIN_KEYTAB));
    }

    @Test
    public void testDecoratedFlinkContainer() {
        final Container resultMainContainer =
                kerberosMountDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();
        assertEquals(2, resultMainContainer.getVolumeMounts().size());

        final VolumeMount keytabVolumeMount =
                resultMainContainer.getVolumeMounts().stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KEYTAB_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        final VolumeMount krb5ConfVolumeMount =
                resultMainContainer.getVolumeMounts().stream()
                        .filter(x -> x.getName().equals(Constants.KERBEROS_KRB5CONF_VOLUME))
                        .collect(Collectors.toList())
                        .get(0);
        assertNotNull(keytabVolumeMount);
        assertNotNull(krb5ConfVolumeMount);
        assertEquals(Constants.KERBEROS_KEYTAB_MOUNT_POINT, keytabVolumeMount.getMountPath());
        assertEquals(
                Constants.KERBEROS_KRB5CONF_MOUNT_DIR + "/krb5.conf",
                krb5ConfVolumeMount.getMountPath());
    }
}
