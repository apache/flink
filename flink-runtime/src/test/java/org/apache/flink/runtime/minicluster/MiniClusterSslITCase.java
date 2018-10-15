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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing the flink cluster using SSL transport for akka remoting.
 */
public class MiniClusterSslITCase extends TestLogger {

	@Test
	public void testStartWithAkkaSslEnabled() throws Exception {
		final Configuration config = new Configuration();

		config.setString(JobManagerOptions.ADDRESS, "127.0.0.1");
		config.setString(TaskManagerOptions.HOST, "127.0.0.1");
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		config.setString(SecurityOptions.SSL_KEYSTORE,
			getClass().getResource("/local127.keystore").getPath());
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_TRUSTSTORE,
			getClass().getResource("/local127.truststore").getPath());

		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.build();
		try (final MiniCluster cluster = new MiniCluster(cfg)) {
			cluster.start();
			assertTrue(cluster.isRunning());
		}
	}

	@Test
	public void testFailedToStartSslEnabledAkkaWithTwoProtocolsSet() {
		final Configuration config = new Configuration();

		config.setString(JobManagerOptions.ADDRESS, "127.0.0.1");
		config.setString(TaskManagerOptions.HOST, "127.0.0.1");
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		config.setString(SecurityOptions.SSL_KEYSTORE,
			getClass().getResource("/local127.keystore").getPath());
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_TRUSTSTORE,
			getClass().getResource("/local127.truststore").getPath());

		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_ALGORITHMS, "TLSv1,TLSv1.1");

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.build();
		try (final MiniCluster cluster = new MiniCluster(cfg)) {
			cluster.start();
			fail();
		} catch (Exception e) {
			findThrowable(e, IOException.class);
			findThrowableWithMessage(e, "Unable to open BLOB Server in specified port range: 0");
		}
	}

	@Test
	public void testStartWithAkkaSslDisabled() throws Exception {
		final Configuration config = new Configuration();

		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false);

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.build();
		try (final MiniCluster cluster = new MiniCluster(cfg)) {
			cluster.start();
			assertTrue(cluster.isRunning());
		}
	}

	@Test
	public void testFailedToStartWithInvalidSslKeystoreConfigured() {
		final Configuration config = new Configuration();

		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setString(AkkaOptions.ASK_TIMEOUT, "2 s");

		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		config.setString(SecurityOptions.SSL_KEYSTORE, "invalid.keystore");
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		config.setString(SecurityOptions.SSL_TRUSTSTORE, "invalid.keystore");
		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.build();
		try (final MiniCluster cluster = new MiniCluster(cfg)) {
			cluster.start();
			fail();
		} catch (Exception e) {
			findThrowable(e, IOException.class);
			findThrowableWithMessage(e, "Failed to initialize SSL for the blob server");
		}
	}

	@Test
	public void testFailedToStartWithMissingMandatorySslConfiguration() {
		final Configuration config = new Configuration();

		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setString(AkkaOptions.ASK_TIMEOUT, "2 s");

		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setConfiguration(config)
			.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
			.build();
		try (final MiniCluster cluster = new MiniCluster(cfg)) {
			cluster.start();
			fail();
		} catch (Exception e) {
			findThrowable(e, IOException.class);
			findThrowableWithMessage(e, "Failed to initialize SSL for the blob server");
		}
	}
}
