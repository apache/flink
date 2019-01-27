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

package org.apache.flink.yarn.failover;

import org.apache.flink.yarn.YarnTestBase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.EnumSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JobMaster failover test streaming case.
 */
public class StreamITCase extends YarnJobMasterFailoverTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(StreamITCase.class);

	private static final String logDir = "jm-failover-StreamITCase";

	@BeforeClass
	public static void setup() {
		YarnJobMasterFailoverTestBase.startHighAvailabilityService();
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, logDir);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@Test
	public void test() throws Exception {
		final Pattern jmCheckerBefore = Pattern.compile("(Source|Sink).*switched from DEPLOYING to RUNNING");
		final Pattern jmCheckerAfter = Pattern.compile("Job.*switched from state RECONCILING to RUNNING.");
		final Pattern tmChecker = Pattern.compile("(Source|Sink).*switched from RUNNING to (FAILED|CANCELLING)");

		final Runner runner = startSession();

		waitUntilCondition(() -> getRunningContainers() >= 2, TIMEOUT.fromNow());

		final YarnClient yarnClient = getYarnClient();
		Assert.assertNotNull(yarnClient);

		Assert.assertEquals(1, yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).size());
		final ApplicationReport report1 = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).get(0);
		Assert.assertEquals(1, report1.getCurrentApplicationAttemptId().getAttemptId());

		final ApplicationId id = report1.getApplicationId();

		submitJob("org.apache.flink.yarn.failover.StreamCase", "stream");

		waitUntilCondition(
			() -> {
				final File jmLog = findFile("..", (dir, name) ->
					name.contains("jobmanager.log") && dir.getAbsolutePath().contains("_01_")
						&& dir.getAbsolutePath().contains(logDir)
						&& dir.getAbsolutePath().contains(fmt.format(id.getId())));
				if (jmLog != null) {
					final String jmLogText = FileUtils.readFileToString(jmLog);
					final Matcher m = jmCheckerBefore.matcher(jmLogText);
					// match 4 times, all vertices running
					return m.find() && m.find() && m.find() && m.find();
				}
				return false;
			},
			TIMEOUT.fromNow());

		// trigger kill
		killJobMaster();

		waitUntilCondition(
			() -> 2 == yarnClient.getApplicationReport(id).getCurrentApplicationAttemptId().getAttemptId(),
			TIMEOUT.fromNow());

		Assert.assertEquals(report1.getTrackingUrl(), yarnClient.getApplicationReport(id).getTrackingUrl());

		waitUntilCondition(
			() -> {
				final File jmLog = findFile("..", (dir, name) ->
					name.contains("jobmanager.log") && dir.getAbsolutePath().contains("_02_")
						&& dir.getAbsolutePath().contains(logDir)
						&& dir.getAbsolutePath().contains(fmt.format(id.getId())));
				if (jmLog != null) {
					final String jmLogText = FileUtils.readFileToString(jmLog);
					final Matcher m = jmCheckerAfter.matcher(jmLogText);
					return m.find();
				}
				return false;
			}, TIMEOUT.fromNow());

		final File tmLog = findFile("..", (dir, name) ->
			name.contains("taskmanager.log") && dir.getAbsolutePath().contains("_01_000003")
				&& dir.getAbsolutePath().contains(logDir)
				&& dir.getAbsolutePath().contains(fmt.format(id.getId())));

		Assert.assertNotNull(tmLog);
		final Matcher m = tmChecker.matcher(FileUtils.readFileToString(tmLog));

		// no failover
		Assert.assertFalse(m.find());

		yarnClient.killApplication(id);
		runner.sendStop();

		// wait for the thread to stop
		runner.join();
	}
}
