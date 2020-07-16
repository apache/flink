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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.kubernetes.configuration.volume.EmptyDirVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.HostPathVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeSpecification;
import org.apache.flink.kubernetes.configuration.volume.NFSVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.PVCVolumeConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KubernetesUtils}.
 */
public class KubernetesUtilsTest extends TestLogger {

	@Test
	public void testParsePortRange() {
		final Configuration cfg = new Configuration();
		cfg.set(BlobServerOptions.PORT, "50100-50200");
		try {
			KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT);
			fail("Should fail with an exception.");
		} catch (FlinkRuntimeException e) {
			assertThat(
				e.getMessage(),
				containsString(BlobServerOptions.PORT.key() + " should be specified to a fixed port. Do not support a range of ports."));
		}
	}

	@Test
	public void testParsePortNull() {
		final Configuration cfg = new Configuration();
		ConfigOption<String> testingPort = ConfigOptions.key("test.port").stringType().noDefaultValue();
		try {
			KubernetesUtils.parsePort(cfg, testingPort);
			fail("Should fail with an exception.");
		} catch (NullPointerException e) {
			assertThat(
				e.getMessage(),
				containsString(testingPort.key() + " should not be null."));
		}
	}

	@Test
	public void testCheckWithDynamicPort() {
		testCheckAndUpdatePortConfigOption("0", "6123", "6123");
	}

	@Test
	public void testCheckWithFixedPort() {
		testCheckAndUpdatePortConfigOption("6123", "16123", "6123");
	}

	@Test
	public void testParseDefaultSubPathAndReadonly() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "hostPath");
		properties.put("name", "volumeName");
		properties.put("options-path", "/hostPath");
		properties.put("mount-path", "/tmp/mount");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertEquals("", volumeSpec.getMountSubPath());
		assertEquals(false, volumeSpec.getMountReadOnly());
	}

	@Test
	public void testParseHostPathVolumeConfig() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "hostPath");
		properties.put("name", "volumeName");
		properties.put("options-path", "/hostPath");
		properties.put("mount-path", "/tmp/mount");
		properties.put("mount-subPath", "/tmp/subPath");
		properties.put("mount-readOnly", "true");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertEquals("/tmp/subPath", volumeSpec.getMountSubPath());
		assertEquals(true, volumeSpec.getMountReadOnly());
		assertTrue(volumeSpec.getVolumeConfig() instanceof HostPathVolumeConfig);
		assertEquals("/hostPath", ((HostPathVolumeConfig) volumeSpec.getVolumeConfig()).getHostPath());
	}

	@Test
	public void testParsePVCVolumeConfig() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "persistentVolumeClaim");
		properties.put("name", "volumeName");
		properties.put("options-claimName", "claimName");
		properties.put("mount-path", "/tmp/mount");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertTrue(volumeSpec.getVolumeConfig() instanceof PVCVolumeConfig);
		assertEquals("claimName", ((PVCVolumeConfig) volumeSpec.getVolumeConfig()).getClaimName());
	}

	@Test
	public void testParseEmptyDirVolumeConfig() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "emptyDir");
		properties.put("name", "volumeName");
		properties.put("options-medium", "memory");
		properties.put("options-sizeLimit", "2G");
		properties.put("mount-path", "/tmp/mount");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertTrue(volumeSpec.getVolumeConfig() instanceof EmptyDirVolumeConfig);
		assertEquals("memory", ((EmptyDirVolumeConfig) volumeSpec.getVolumeConfig()).getMedium());
		assertEquals("2G", ((EmptyDirVolumeConfig) volumeSpec.getVolumeConfig()).getSizeLimit());
	}

	@Test
	public void testPareEmptyDirVolumeConfigWithNullOptions() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "emptyDir");
		properties.put("name", "volumeName");
		properties.put("mount-path", "/tmp/mount");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertTrue(volumeSpec.getVolumeConfig() instanceof EmptyDirVolumeConfig);
		assertEquals("", ((EmptyDirVolumeConfig) volumeSpec.getVolumeConfig()).getMedium());
		assertNull(((EmptyDirVolumeConfig) volumeSpec.getVolumeConfig()).getSizeLimit());
	}

	@Test
	public void testParseNFSVolumeConfig() {
		final Map<String, String> properties = new HashMap<>();
		properties.put("type", "nfs");
		properties.put("name", "volumeName");
		properties.put("options-path", "/tmp/nfs");
		properties.put("options-server", "nfs.example.com");
		properties.put("mount-path", "/tmp/mount");
		KubernetesVolumeSpecification volumeSpec = KubernetesUtils.parseVolumes(properties);
		assertEquals("volumeName", volumeSpec.getVolumeName());
		assertEquals("/tmp/mount", volumeSpec.getMountPath());
		assertTrue(volumeSpec.getVolumeConfig() instanceof NFSVolumeConfig);
		assertEquals("/tmp/nfs", ((NFSVolumeConfig) volumeSpec.getVolumeConfig()).getPath());
		assertEquals("nfs.example.com", ((NFSVolumeConfig) volumeSpec.getVolumeConfig()).getServer());
	}

	private void testCheckAndUpdatePortConfigOption(String port, String fallbackPort, String expectedPort) {
		final Configuration cfg = new Configuration();
		cfg.setString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE, port);
		KubernetesUtils.checkAndUpdatePortConfigOption(
			cfg,
			HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
			Integer.valueOf(fallbackPort));
		assertEquals(expectedPort, cfg.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE));
	}
}
