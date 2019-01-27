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

package org.apache.flink.test.util;

import org.apache.flink.network.yarn.YarnShuffleService;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleService;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;

/**
 * A mock shuffle service server.
 */
public class MockYarnShuffleService {
	private final org.apache.hadoop.conf.Configuration
		hadoopConf = new org.apache.hadoop.conf.Configuration();

	private final String user;

	private final String appId;

	private final int port;

	private ExternalBlockShuffleService shuffleService = null;

	public MockYarnShuffleService(String externalDir, String user, String appId, int port, int threadNum) {
		this.user = user;
		this.appId = appId;
		this.port = port;

		hadoopConf.setStrings(ExternalBlockShuffleServiceOptions.LOCAL_DIRS.key(), "[test]" + externalDir);
		hadoopConf.setLong(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_DIRECT_MEMORY_LIMIT_IN_MB.key(), 200);
		hadoopConf.setInt(ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY.key(), port);
		hadoopConf.setStrings(ExternalBlockShuffleServiceOptions.IO_THREAD_NUM_FOR_DISK_TYPE.key(), "test: " + threadNum);
		hadoopConf.setLong(ExternalBlockShuffleServiceOptions.DISK_SCAN_INTERVAL_IN_MS.key(), 100);
	}

	public int getPort() {
		return port;
	}

	public void start() throws Exception {
		// Postpone the creation of shuffle service till start to avoid startings all the thread pool in advance.
		shuffleService = new ExternalBlockShuffleService(YarnShuffleService.fromHadoopConfiguration(hadoopConf));
		shuffleService.start();
		// shuffle service need to use this message to map appId to user
		shuffleService.initializeApplication(user, appId);
	}

	public void stop() {
		if (shuffleService != null) {
			shuffleService.stopApplication(appId);
			shuffleService.stop();
			shuffleService = null;
		}
	}
}
