/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.concurrent;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Unit tests for {@link ScheduledFutureAdapter}.
 */
public class ScheduledFutureAdapterTest {

	private ScheduledFutureAdapter objectUnderTest;
	private Future<Integer> innerDelegate;
	private AtomicBoolean resultCancel;
	private AtomicBoolean resultIsCancelled;
	private AtomicBoolean resultIsDone;

	@Before
	public void before() throws Exception {
		this.resultCancel = new AtomicBoolean(false);
		this.resultIsCancelled = new AtomicBoolean(false);
		this.resultIsDone = new AtomicBoolean(false);

		this.innerDelegate = spy(new Future<Integer>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return resultCancel.get();
			}

			@Override
			public boolean isCancelled() {
				return resultIsCancelled.get();
			}

			@Override
			public boolean isDone() {
				return resultIsDone.get();
			}

			@Override
			public Integer get() {
				return 4711;
			}

			@Override
			public Integer get(long timeout, TimeUnit unit) {
				return 4711;
			}
		});

		this.objectUnderTest = new ScheduledFutureAdapter<>(innerDelegate, 4200L, TimeUnit.MILLISECONDS);
	}

	@Test
	public void testForwardedMethods() throws Exception {

		Assert.assertEquals(4711, objectUnderTest.get());
		Mockito.verify(innerDelegate).get();

		Assert.assertEquals(4711, objectUnderTest.get(42L, TimeUnit.SECONDS));
		Mockito.verify(innerDelegate).get(42L, TimeUnit.SECONDS);

		Assert.assertEquals(resultCancel.get(), objectUnderTest.cancel(true));
		Mockito.verify(innerDelegate, Mockito.times(1)).cancel(true);

		resultCancel.set(!resultCancel.get());
		Assert.assertEquals(resultCancel.get(), objectUnderTest.cancel(true));
		Mockito.verify(innerDelegate, Mockito.times(2)).cancel(true);

		Assert.assertEquals(resultIsCancelled.get(), objectUnderTest.isCancelled());
		Mockito.verify(innerDelegate, Mockito.times(1)).isCancelled();

		resultIsCancelled.set(!resultIsCancelled.get());
		Assert.assertEquals(resultIsCancelled.get(), objectUnderTest.isCancelled());
		Mockito.verify(innerDelegate, Mockito.times(2)).isCancelled();

		Assert.assertEquals(resultIsDone.get(), objectUnderTest.isDone());
		Mockito.verify(innerDelegate, Mockito.times(1)).isDone();

		resultIsDone.set(!resultIsDone.get());
		Assert.assertEquals(resultIsDone.get(), objectUnderTest.isDone());
		Mockito.verify(innerDelegate, Mockito.times(2)).isDone();
	}

	@Test
	public void testDelay() {

		Assert.assertEquals(4200L, objectUnderTest.getDelay(TimeUnit.MILLISECONDS));
		Assert.assertEquals(4L, objectUnderTest.getDelay(TimeUnit.SECONDS));

		final AtomicLong delayUnits = new AtomicLong();
		Delayed delayed = new Delayed() {
			@Override
			public long getDelay(TimeUnit unit) {
				return delayUnits.get();
			}

			@Override
			public int compareTo(Delayed o) {
				throw new UnsupportedOperationException();
			}
		};

		delayUnits.set(objectUnderTest.getDelay(TimeUnit.MILLISECONDS));
		Assert.assertEquals(0, objectUnderTest.compareTo(delayed));
		delayUnits.set(delayUnits.get() + 1);
		Assert.assertEquals(-1, Integer.signum(objectUnderTest.compareTo(delayed)));
		delayUnits.set(delayUnits.get() - 2);
		Assert.assertEquals(1, Integer.signum(objectUnderTest.compareTo(delayed)));
	}

}
