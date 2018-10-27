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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.NumberFormat;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

/**
 * Tests that verify correct HA behavior.
 */
public class YARNHighAvailabilityITCase extends YarnTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(YARNHighAvailabilityITCase.class);

	@ClassRule
	public static final TemporaryFolder FOLDER = new TemporaryFolder();

	private static final String LOG_DIR = "flink-yarn-tests-ha";
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	private static final Pattern PATTERN = Pattern.compile("(Source|Sink).*switched from DEPLOYING to RUNNING");
	private static final FiniteDuration TIMEOUT = FiniteDuration.apply(200000L, TimeUnit.MILLISECONDS);

	private static TestingServer zkServer;
	private static String storageDir;
	private static String zkQuorum;

	@BeforeClass
	public static void setup() {
		try {
			zkServer = new TestingServer();
			zkServer.start();

			storageDir = FOLDER.newFolder().getAbsolutePath();
			zkQuorum = zkServer.getConnectString();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Cannot start ZooKeeper Server.");
		}

		FORMAT.setGroupingUsed(false);
		FORMAT.setMinimumIntegerDigits(4);

		// startYARNWithConfig should be implemented by subclass
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, LOG_DIR);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (zkServer != null) {
			zkServer.stop();
			zkServer = null;
		}
	}

	@Test
	public void testKillJobManager() throws Exception {
		final Runner clusterRunner = startWithArgs(
			new String[]{
				"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-n", "2",
				"-jm", "768",
				"-tm", "1024",
				"-s", "1",
				"-nm", "test-cluster",
				"-D" + TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + "=128",
				"-D" + YarnConfigOptions.APPLICATION_ATTEMPTS.key() + "=10",
				"-D" + HighAvailabilityOptions.HA_MODE.key() + "=ZOOKEEPER",
				"-D" + HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + storageDir,
				"-D" + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "=" + zkQuorum,
				"-D" + ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY.key() + "=3 s",
				"-D" + ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS + "=10",
				"--detached"
			},
			"Flink JobManager is now running",
			RunTypes.YARN_SESSION);

		// before checking any strings outputted by the CLI, first give it time to return
		clusterRunner.join();

		// actually run a program, otherwise we wouldn't necessarily see any TaskManagers
		// be brought up
		final File testingJar =
			YarnTestBase.findFile("..", new YarnTestUtils.TestJarFinder("flink-yarn-tests"));
		final String job = "org.apache.flink.yarn.testjob.StreamCase";

		Runner jobRunner = startWithArgs(new String[]{"run",
			"--detached",
			"-c", job,
			testingJar.getAbsolutePath(),
			"-yD", HighAvailabilityOptions.HA_MODE.key() + "=ZOOKEEPER",
			"-yD", HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + storageDir,
			"-yD", HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "=" + zkQuorum,
		}, "Job has been submitted with JobID", RunTypes.CLI_FRONTEND);

		jobRunner.join();

		while (getRunningContainers() < 3) {
			sleep(500);
		}

		final YarnClient yarnClient = getYarnClient();
		Assert.assertNotNull(yarnClient);

		Assert.assertEquals(1, yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).size());
		final ApplicationReport report1 = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING)).get(0);
		Assert.assertEquals(1, report1.getCurrentApplicationAttemptId().getAttemptId());

		final ApplicationId id = report1.getApplicationId();

		waitUntilCondition(
			() -> {
				final File jmLog = findFile("..", (dir, name) ->
					name.contains("jobmanager.log") && dir.getAbsolutePath().contains("_01_")
						&& dir.getAbsolutePath().contains(LOG_DIR)
						&& dir.getAbsolutePath().contains(FORMAT.format(id.getId())));
				if (jmLog != null) {
					final String jmLogText = FileUtils.readFileToString(jmLog);
					final Matcher m = PATTERN.matcher(jmLogText);
					// match 4 times, all vertices running
					return m.find() && m.find() && m.find() && m.find();
				}
				return false;
			}, TIMEOUT.fromNow());

		Runtime.getRuntime().exec(new String[]{
			"/bin/sh", "-c", "kill $(ps aux | grep -v bash | grep jobmanager | grep -v grep | FS=' \\t' awk '{print $2}')"
		});

		while (yarnClient.getApplicationReport(id).getCurrentApplicationAttemptId().getAttemptId() < 2) {
			sleep(500);
		}

		Assert.assertEquals(report1.getTrackingUrl(), yarnClient.getApplicationReport(id).getTrackingUrl());

		waitUntilCondition(
			() -> {
				final File jmLog = findFile("..", (dir, name) ->
					name.contains("jobmanager.log") && dir.getAbsolutePath().contains("_02_")
						&& dir.getAbsolutePath().contains(LOG_DIR)
						&& dir.getAbsolutePath().contains(FORMAT.format(id.getId())));
				if (jmLog != null) {
					final String jmLogText = FileUtils.readFileToString(jmLog);
					final Matcher m = PATTERN.matcher(jmLogText);
					// match 4 times, all vertices running
					return m.find() && m.find() && m.find() && m.find();
				}
				return false;
			}, TIMEOUT.fromNow());

		yarnClient.killApplication(id);

		while (yarnClient.getApplications(EnumSet.of(YarnApplicationState.KILLED)).size() == 0 &&
			yarnClient.getApplications(EnumSet.of(YarnApplicationState.FINISHED)).size() == 0) {
			sleep(500);
		}
	}

	private void waitUntilCondition(SupplierWithException<Boolean, Exception> condition, Deadline timeout) {
		while (timeout.hasTimeLeft()) {
			try {
				if (condition.get()) {
					return;
				}
				Thread.sleep(Math.min(500, timeout.timeLeft().toMillis()));
			} catch (Exception e) {
				// do nothing
			}
		}

		throw new IllegalStateException("Condition not arrived within deadline.");
	}
}
