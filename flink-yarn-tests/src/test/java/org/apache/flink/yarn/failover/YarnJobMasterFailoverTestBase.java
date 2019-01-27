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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.yarn.YarnTestBase;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.text.NumberFormat;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

/**
 * Test for {@link org.apache.flink.runtime.jobmaster.JobMaster} failover on YARN.
 */
public abstract class YarnJobMasterFailoverTestBase extends YarnTestBase {
	@ClassRule
	public static final TemporaryFolder FOLDER = new TemporaryFolder();

	protected static final FiniteDuration TIMEOUT = FiniteDuration.apply(200000L, TimeUnit.MILLISECONDS);

	private static TestingServer zkServer;
	private static String storageDir;
	private static String zkQuorum;

	protected static NumberFormat fmt = NumberFormat.getInstance();

	protected static void startHighAvailabilityService() {
		try {
			zkServer = new TestingServer();
			zkServer.start();
			storageDir = FOLDER.newFolder().getAbsolutePath();
			zkQuorum = zkServer.getConnectString();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Could not start ZooKeeper testing cluster.");
		}

		fmt.setGroupingUsed(false);
		fmt.setMinimumIntegerDigits(4);

		// startYARNWithConfig should be implemented by subclass
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (zkServer != null) {
			zkServer.stop();
			zkServer = null;
		}
	}

	static void waitUntilCondition(SupplierWithException<Boolean, Exception> condition, Deadline timeout) {
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

	static void killJobMaster() throws Exception {
		Runtime.getRuntime().exec(new String[] {
			"/bin/sh", "-c", "kill $(ps aux | grep -v bash | grep jobmanager | grep -v grep | FS=' \\t' awk '{print $2}')"
		});
	}

	static void killTaskExecutor() throws Exception {
		Runtime.getRuntime().exec(new String[] {
			"/bin/sh", "-c", "kill $(ps aux | grep -v bash | grep taskmanager | grep _000002 | grep -v grep | FS=' \\t' awk '{print $2}')"
		});
	}

	Runner startSession() throws Exception {
		return startWithArgs(
			new String[]{
				"-j", flinkUberjar.getAbsolutePath(),
				"-t", flinkLibFolder.getAbsolutePath(),
				"-n", "2",
				"-jm", "768",
				"-tm", "1024",
				"-s", "1",
				"-nm", "test-cluster",
				"-D" + TaskManagerOptions.TASK_MANAGER_CORE.key() + "=2",
				"-D" + TaskManagerOptions.MANAGED_MEMORY_SIZE.key() + "=128",
				"-D" + YarnConfigOptions.APPLICATION_ATTEMPTS.key() + "=2",
				"-D" + HighAvailabilityOptions.HA_MODE.key() + "=ZOOKEEPER",
				"-D" + HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + storageDir,
				"-D" + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "=" + zkQuorum,
				"-D" + JobManagerOptions.OPERATION_LOG_STORE.key() + "=" + "filesystem",
				"-D" + ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY.key() + "=3 s",
				"-D" + ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS + "=1"
			},
			"Flink JobManager is now running",
			RunTypes.YARN_SESSION);
	}

	Runner submitJob(String caseclass, String casename) throws Exception {
		final File uberjar = findFile("..", (dir, name) ->
			name.startsWith("flink-yarn-tests_2.11") && name.endsWith("-tests.jar"));

		return startWithArgs(
			new String[] {
				"run",
				"-c", caseclass,
				uberjar.getAbsolutePath(),
				"-yD", HighAvailabilityOptions.HA_MODE.key() + "=ZOOKEEPER",
				"-yD", HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + storageDir,
				"-yD", HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "=" + zkQuorum,
				"--casename", casename
			},
			"",
			RunTypes.CLI_FRONTEND
		);
	}
}
