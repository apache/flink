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

package org.apache.flink.runtime.jobmanager.web;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.web.util.DefaultConfigKeyValues;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConfigurationViewTest {

	@Test
	public void testCompleteness(){

		//these keys are never used.
		Set<String> unused = new HashSet<String>(Arrays.asList("TASK_MANAGER_NET_NUM_IN_THREADS_KEY",
			"TASK_MANAGER_NET_NUM_OUT_THREADS_KEY",
			"TASK_MANAGER_NET_NETTY_LOW_WATER_MARK",
			"TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK",
			"TASK_MANAGER_HEARTBEAT_INTERVAL_KEY"));

		Map<String, Object> pairs = DefaultConfigKeyValues.getDefaultConfig(new Configuration());

		for (Field f : ConfigConstants.class.getFields()) {
			String name = f.getName();
			if (!name.startsWith("DEFAULT") && !unused.contains(name)) {
				try {
					String value = (String)f.get(null);
					if (!pairs.keySet().contains(value)) {
						Assert.fail(value + " is not included in the configuration overview.");
					}
				} catch (IllegalAccessException iae) {
					//pass
				}
			}
		}
	}
}
