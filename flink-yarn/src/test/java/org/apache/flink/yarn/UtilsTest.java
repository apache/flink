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

package org.apache.flink.yarn;

import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Utils}.
 */
public class UtilsTest extends TestLogger {

	private static final String YARN_RM_ARBITRARY_SCHEDULER_CLAZZ =
			"org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDeleteApplicationFiles() throws Exception {
		final Path applicationFilesDir = temporaryFolder.newFolder(".flink").toPath();
		Files.createFile(applicationFilesDir.resolve("flink.jar"));
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			assertThat(files.count(), equalTo(1L));
		}
		try (Stream<Path> files = Files.list(applicationFilesDir)) {
			assertThat(files.count(), equalTo(1L));
		}

		Utils.deleteApplicationFiles(Collections.singletonMap(
			YarnConfigKeys.FLINK_YARN_FILES,
			applicationFilesDir.toString()));
		try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
			assertThat(files.count(), equalTo(0L));
		}
	}

	@Test
	public void testGetUnitResource() {
		final int minMem = 64;
		final int minVcore = 1;
		final int incMem = 512;
		final int incVcore = 2;
		final int incMemLegacy = 1024;
		final int incVcoreLegacy = 4;

		YarnConfiguration yarnConfig = new YarnConfiguration();
		yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, minMem);
		yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, minVcore);
		yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_LEGACY_KEY, incMemLegacy);
		yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_LEGACY_KEY, incVcoreLegacy);

		verifyUnitResourceVariousSchedulers(yarnConfig, minMem, minVcore, incMemLegacy, incVcoreLegacy);

		yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_KEY, incMem);
		yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_KEY, incVcore);

		verifyUnitResourceVariousSchedulers(yarnConfig, minMem, minVcore, incMem, incVcore);
	}

	private static void verifyUnitResourceVariousSchedulers(YarnConfiguration yarnConfig, int minMem, int minVcore, int incMem, int incVcore) {
		yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_FAIR_SCHEDULER_CLAZZ);
		verifyUnitResource(yarnConfig, incMem, incVcore);

		yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_SLS_FAIR_SCHEDULER_CLAZZ);
		verifyUnitResource(yarnConfig, incMem, incVcore);

		yarnConfig.set(YarnConfiguration.RM_SCHEDULER, YARN_RM_ARBITRARY_SCHEDULER_CLAZZ);
		verifyUnitResource(yarnConfig, minMem, minVcore);
	}

	private static void verifyUnitResource(YarnConfiguration yarnConfig, int expectedMem, int expectedVcore) {
		final Resource unitResource = Utils.getUnitResource(yarnConfig);
		assertThat(unitResource.getMemory(), is(expectedMem));
		assertThat(unitResource.getVirtualCores(), is(expectedVcore));
	}
}
