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

public class FlinkJedisConfigBaseTest extends TestLogger {

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionIfTimeOutIsNegative(){
		new TestConfig(-1, 0, 0, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionIfMaxTotalIsNegative(){
		new TestConfig(1, -1, 0, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionIfMaxIdleIsNegative(){
		new TestConfig(0, 0, -1, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowIllegalArgumentExceptionIfMinIdleIsNegative(){
		new TestConfig(0, 0, 0, -1);
	}

	private class TestConfig extends FlinkJedisConfigBase{

		protected TestConfig(int connectionTimeout, int maxTotal, int maxIdle, int minIdle) {
			super(connectionTimeout, maxTotal, maxIdle, minIdle);
		}
	}
}
