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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimerServiceTest extends TestLogger {
	/**
	 * Test all timeouts registered can be unregistered
	 * @throws Exception
   */
	@Test
	@SuppressWarnings("unchecked")
	public void testUnregisterAllTimeouts() throws Exception {
		// Prepare all instances.
		ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
		ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
		when(scheduledExecutorService.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
			.thenReturn(scheduledFuture);
		TimerService<AllocationID> timerService = new TimerService<>(scheduledExecutorService, 100L);
		TimeoutListener<AllocationID> listener = mock(TimeoutListener.class);

		timerService.start(listener);

		// Invoke register and unregister.
		timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);
		timerService.registerTimeout(new AllocationID(), 10, TimeUnit.SECONDS);

		timerService.unregisterAllTimeouts();

		// Verify.
		Map<?, ?> timeouts = (Map<?, ?>) Whitebox.getInternalState(timerService, "timeouts");
		assertTrue(timeouts.isEmpty());
		verify(scheduledFuture, times(2)).cancel(true);
	}

}
