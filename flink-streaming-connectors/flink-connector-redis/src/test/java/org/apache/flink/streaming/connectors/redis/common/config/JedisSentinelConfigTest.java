/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.redis.common.config;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class JedisSentinelConfigTest extends TestLogger {

	public static final String MASTER_NAME = "test-master";

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointExceptionIfMasterValueIsNull(){
		FlinkJedisSentinelConfig.Builder builder = new FlinkJedisSentinelConfig.Builder();
		Set<String> sentinels = new HashSet<>();
		sentinels.add("127.0.0.1");
		builder.setSentinels(sentinels).build();
	}

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointExceptionIfSentinelsValueIsNull(){
		FlinkJedisSentinelConfig.Builder builder = new FlinkJedisSentinelConfig.Builder();
		builder.setMasterName(MASTER_NAME).build();
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowNullPointExceptionIfSentinelsValueIsEmpty(){
		FlinkJedisSentinelConfig.Builder builder = new FlinkJedisSentinelConfig.Builder();
		Set<String> sentinels = new HashSet<>();
		builder.setMasterName(MASTER_NAME).setSentinels(sentinels).build();
	}
}
