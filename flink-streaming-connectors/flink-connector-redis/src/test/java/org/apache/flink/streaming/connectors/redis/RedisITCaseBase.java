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
package org.apache.flink.streaming.connectors.redis;

import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import redis.embedded.RedisServer;

import java.io.IOException;

import static org.apache.flink.util.NetUtils.getAvailablePort;

public abstract class RedisITCaseBase extends StreamingMultipleProgramsTestBase {

	public static final int REDIS_PORT = getAvailablePort();
	public static final String REDIS_HOST = "127.0.0.1";

	private static RedisServer redisServer;

	@BeforeClass
	public static void createRedisServer() throws IOException, InterruptedException {
		redisServer = new RedisServer(REDIS_PORT);
		redisServer.start();
	}

	@AfterClass
	public static void stopRedisServer(){
		redisServer.stop();
	}
}
