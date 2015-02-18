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

package org.apache.flink.streaming.api.windowing.policy;

import static org.junit.Assert.*;

import org.junit.Test;

public class TumblingEvictionPolicyTest {

	@Test
	public void testTumblingEviction() {
		EvictionPolicy<Integer> policy = new TumblingEvictionPolicy<Integer>();

		int counter = 0;

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < i; j++) {
				assertEquals(0, policy.notifyEviction(0, false, counter++));
			}
			assertEquals(counter, policy.notifyEviction(0, true, counter));
			counter = 1;
		}

		assertEquals(new TumblingEvictionPolicy<Integer>(), new TumblingEvictionPolicy<Integer>());
	}

}
