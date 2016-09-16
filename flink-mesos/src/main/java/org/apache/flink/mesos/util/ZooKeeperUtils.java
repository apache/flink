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

package org.apache.flink.mesos.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.configuration.Configuration;

public class ZooKeeperUtils {

	/**
	 * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper
	 * quorum.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link CuratorFramework} instance
	 */
	@SuppressWarnings("unchecked")
	public static CuratorFramework startCuratorFramework(Configuration configuration) {

		// using late-binding as a workaround for shaded curator dependency of flink-runtime
		Object client = org.apache.flink.runtime.util.ZooKeeperUtils.startCuratorFramework(configuration);
		return (CuratorFramework) client;
	}
}
