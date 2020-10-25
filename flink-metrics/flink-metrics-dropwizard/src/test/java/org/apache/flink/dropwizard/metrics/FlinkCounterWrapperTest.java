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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.util.TestCounter;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the FlinkCounterWrapper.
 */
public class FlinkCounterWrapperTest {

	@Test
	public void testWrapperIncDec() {
		Counter counter = new TestCounter();
		counter.inc();

		FlinkCounterWrapper wrapper = new FlinkCounterWrapper(counter);
		assertEquals(1L, wrapper.getCount());
		wrapper.dec();
		assertEquals(0L, wrapper.getCount());
		wrapper.inc(2);
		assertEquals(2L, wrapper.getCount());
		wrapper.dec(2);
		assertEquals(0L, wrapper.getCount());
	}
}
