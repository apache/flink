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

package org.apache.flink.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link JavaGcCleanerWrapper}.
 */
public class JavaGcCleanerWrapperTest {
	@Test
	public void testCleanOperationRunsOnlyOnceEitherOnGcOrExplicitly() throws InterruptedException {
		AtomicInteger callCounter = new AtomicInteger();
		Runnable cleaner = JavaGcCleanerWrapper.create(new Object(), callCounter::incrementAndGet);
		System.gc(); // not guaranteed to be run always but should in practice
		Thread.sleep(10); // more chance for GC to run
		cleaner.run();
		cleaner.run();
		assertThat(callCounter.get(), is(1));
	}
}
