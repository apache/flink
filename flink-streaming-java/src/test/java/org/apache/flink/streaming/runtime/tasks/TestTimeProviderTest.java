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

package org.apache.flink.streaming.runtime.tasks;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestTimeProviderTest {

	@Test
	public void testTimerSorting() throws Exception {

		final List<Long> result = new ArrayList<>();

		TestTimeServiceProvider provider = new TestTimeServiceProvider();

		provider.registerTimer(45, new Runnable() {
			@Override
			public void run() {
				result.add(45L);
			}
		});

		provider.registerTimer(50, new Runnable() {
			@Override
			public void run() {
				result.add(50L);
			}
		});

		provider.registerTimer(30, new Runnable() {
			@Override
			public void run() {
				result.add(30L);
			}
		});

		provider.registerTimer(50, new Runnable() {
			@Override
			public void run() {
				result.add(50L);
			}
		});

		provider.setCurrentTime(100);

		long seen = 0;
		for (Long l: result) {
			Assert.assertTrue(l >= seen);
			seen = l;
		}
	}
}
