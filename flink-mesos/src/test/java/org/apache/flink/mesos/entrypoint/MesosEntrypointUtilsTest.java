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

package org.apache.flink.mesos.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.overlays.ContainerOverlay;
import org.apache.flink.runtime.clusterframework.overlays.FlinkDistributionOverlay;
import org.apache.flink.runtime.clusterframework.overlays.FlinkJobOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.HadoopUserOverlay;
import org.apache.flink.runtime.clusterframework.overlays.KeytabOverlay;
import org.apache.flink.runtime.clusterframework.overlays.Krb5ConfOverlay;
import org.apache.flink.runtime.clusterframework.overlays.SSLStoreOverlay;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_PLUGINS_DIR;


/**
 * Test for {@link MesosEntrypointUtils}.
 */
public class MesosEntrypointUtilsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();


	File binFolder;
	File libFolder;
	File pluginsFolder;
	File confFolder;

	@Before
	public void init() throws IOException {
		binFolder = temporaryFolder.newFolder("bin");
		libFolder = temporaryFolder.newFolder("lib");
		pluginsFolder = temporaryFolder.newFolder("plugins");
		confFolder = temporaryFolder.newFolder("conf");

		// adjust the test environment for the purposes of this test
		Map<String, String> map = new HashMap<>(System.getenv());
		map.put(ENV_FLINK_BIN_DIR, binFolder.getAbsolutePath());
		map.put(ENV_FLINK_LIB_DIR, libFolder.getAbsolutePath());
		map.put(ENV_FLINK_PLUGINS_DIR, pluginsFolder.getAbsolutePath());
		map.put(ENV_FLINK_CONF_DIR, confFolder.getAbsolutePath());
		CommonTestUtils.setEnv(map);
	}

	@Test
	public void testBuildSessionOverlays() throws IOException {

		Configuration configuration = new Configuration();
		List<ContainerOverlay> containerOverlays =
			MesosEntrypointUtils.buildOverlays(new Configuration(), false);
		List<Class> expectedClass =
			Arrays.asList(HadoopConfOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				HadoopUserOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				KeytabOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				Krb5ConfOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				SSLStoreOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				FlinkDistributionOverlay.newBuilder().fromEnvironment(configuration).build().getClass()
			);

		List<Class> results = containerOverlays.stream().map(ContainerOverlay::getClass).collect(Collectors.toList());

		assert(CollectionUtils.isEqualCollection(expectedClass, results));
	}

	@Test
	public void testBuildJobOverlays() throws IOException {

		Configuration configuration = new Configuration();
		List<ContainerOverlay> containerOverlays =
			MesosEntrypointUtils.buildOverlays(new Configuration(), true);
		List<Class> expectedClass =
			Arrays.asList(HadoopConfOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				HadoopUserOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				KeytabOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				Krb5ConfOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				SSLStoreOverlay.newBuilder().fromEnvironment(configuration).build().getClass(),
				FlinkJobOverlay.newBuilder().fromEnvironment(configuration).build().getClass()
			);

		List<Class> results = containerOverlays.stream().map(ContainerOverlay::getClass).collect(Collectors.toList());

		assert(CollectionUtils.isEqualCollection(expectedClass, results));
	}
}
