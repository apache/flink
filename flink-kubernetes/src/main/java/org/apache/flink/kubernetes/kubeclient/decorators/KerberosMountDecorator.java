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
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mount the custom Kerberos Configuration and Credential to the JobManager(s)/TaskManagers.
 */
public class KerberosMountDecorator extends AbstractKubernetesStepDecorator {
	private static final Logger LOG = LoggerFactory.getLogger(KerberosMountDecorator.class);

	private final AbstractKubernetesParameters kubernetesParameters;
	private final SecurityConfiguration securityConfig;

	public KerberosMountDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
		this.securityConfig = new SecurityConfiguration(kubernetesParameters.getFlinkConfiguration());
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		PodBuilder podBuilder = new PodBuilder(flinkPod.getPod());
		ContainerBuilder containerBuilder = new ContainerBuilder(flinkPod.getMainContainer());

		if (!StringUtils.isNullOrWhitespaceOnly(securityConfig.getKeytab()) && !StringUtils.isNullOrWhitespaceOnly(securityConfig.getPrincipal())) {
			podBuilder = podBuilder
				.editOrNewSpec()
					.addNewVolume()
						.withName(Constants.KERBEROS_KEYTAB_VOLUME)
						.withNewSecret()
							.withSecretName(getKerberosKeytabSecretName(kubernetesParameters.getClusterId()))
							.endSecret()
					.endVolume()
				.endSpec();

			containerBuilder = containerBuilder
				.addNewVolumeMount()
					.withName(Constants.KERBEROS_KEYTAB_VOLUME)
					.withMountPath(Constants.KERBEROS_KEYTAB_MOUNT_POINT)
				.endVolumeMount();
		}

		if (!StringUtils.isNullOrWhitespaceOnly(kubernetesParameters.getFlinkConfiguration().get(SecurityOptions.KERBEROS_KRB5_PATH))) {
			final File krb5Conf = new File(kubernetesParameters.getFlinkConfiguration().get(SecurityOptions.KERBEROS_KRB5_PATH));
			podBuilder = podBuilder
				.editOrNewSpec()
				.addNewVolume()
					.withName(Constants.KERBEROS_KRB5CONF_VOLUME)
					.withNewConfigMap()
						.withName(getKerberosKrb5confConfigMapName(kubernetesParameters.getClusterId()))
						.withItems(new KeyToPathBuilder()
							.withKey(krb5Conf.getName())
							.withPath(krb5Conf.getName())
							.build())
					.endConfigMap()
				.endVolume()
				.endSpec();

			containerBuilder = containerBuilder
				.addNewVolumeMount()
					.withName(Constants.KERBEROS_KRB5CONF_VOLUME)
					.withMountPath(Constants.KERBEROS_KRB5CONF_MOUNT_DIR + "/krb5.conf")
					.withSubPath("krb5.conf")
				.endVolumeMount();
		}

		return new FlinkPod(podBuilder.build(), containerBuilder.build());
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {

		final List<HasMetadata> resources = new ArrayList<>();

		if (!StringUtils.isNullOrWhitespaceOnly(securityConfig.getKeytab()) && !StringUtils.isNullOrWhitespaceOnly(securityConfig.getPrincipal())) {
			final File keytab = new File(securityConfig.getKeytab());
			if (!keytab.exists()) {
				LOG.warn("Could not found the kerberos keytab file in {}.", keytab.getAbsolutePath());
			} else {
				resources.add(new SecretBuilder()
					.withNewMetadata()
						.withName(getKerberosKeytabSecretName(kubernetesParameters.getClusterId()))
					.endMetadata()
					.addToData(keytab.getName(), Base64.getEncoder().encodeToString(Files.toByteArray(keytab)))
					.build());

				// Set keytab path in the container. One should make sure this decorator is triggered before FlinkConfMountDecorator.
				kubernetesParameters.getFlinkConfiguration().set(SecurityOptions.KERBEROS_LOGIN_KEYTAB, String.format("%s/%s", Constants.KERBEROS_KEYTAB_MOUNT_POINT, keytab.getName()));
			}
		}

		if (!StringUtils.isNullOrWhitespaceOnly(kubernetesParameters.getFlinkConfiguration().get(SecurityOptions.KERBEROS_KRB5_PATH))) {
			final File krb5Conf = new File(kubernetesParameters.getFlinkConfiguration().get(SecurityOptions.KERBEROS_KRB5_PATH));
			if (!krb5Conf.exists()) {
				LOG.warn("Could not found the kerberos config file in {}.", krb5Conf.getAbsolutePath());
			} else {
				resources.add(
					new ConfigMapBuilder()
						.withNewMetadata()
							.withName(getKerberosKrb5confConfigMapName(kubernetesParameters.getClusterId()))
						.endMetadata()
						.addToData(krb5Conf.getName(), Files.toString(krb5Conf, StandardCharsets.UTF_8))
						.build());
			}
		}
		return resources;
	}

	public static String getKerberosKeytabSecretName(String clusterId) {
		return Constants.KERBEROS_KEYTAB_SECRET_PREFIX + clusterId;
	}

	public static String getKerberosKrb5confConfigMapName(String clusterID) {
		return Constants.KERBEROS_KRB5CONF_CONFIG_MAP_PREFIX + clusterID;
	}
}
