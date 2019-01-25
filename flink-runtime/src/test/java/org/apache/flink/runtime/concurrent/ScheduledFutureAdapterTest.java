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

import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link ScheduledFutureAdapter}.
 */
public class ScheduledFutureAdapterTest extends TestLogger {

	private ScheduledFutureAdapter<Integer> objectUnderTest;
	private TestFuture innerDelegate;

	@Before
	public void before() throws Exception {
		this.innerDelegate = new TestFuture();
		this.objectUnderTest = new ScheduledFutureAdapter<>(innerDelegate, 4200L, TimeUnit.MILLISECONDS);
	}

	@Test
	public void testForwardedMethods() throws Exception {

		Assert.assertEquals((Integer) 4711, objectUnderTest.get());
		Assert.assertEquals(1, innerDelegate.getGetInvocationCount());

		Assert.assertEquals((Integer) 4711, objectUnderTest.get(42L, TimeUnit.SECONDS));
		Assert.assertEquals(1, innerDelegate.getGetTimeoutInvocationCount());

		Assert.assertEquals(innerDelegate.isCancelExpected(), objectUnderTest.cancel(true));
		Assert.assertEquals(1, innerDelegate.getCancelInvocationCount());

		innerDelegate.setCancelResult(!innerDelegate.isCancelExpected());
		Assert.assertEquals(innerDelegate.isCancelExpected(), objectUnderTest.cancel(true));
		Assert.assertEquals(2, innerDelegate.getCancelInvocationCount());

		Assert.assertEquals(innerDelegate.isCancelledExpected(), objectUnderTest.isCancelled());
		Assert.assertEquals(1, innerDelegate.getIsCancelledInvocationCount());

		innerDelegate.setIsCancelledResult(!innerDelegate.isCancelledExpected());
		Assert.assertEquals(innerDelegate.isCancelledExpected(), objectUnderTest.isCancelled());
		Assert.assertEquals(2, innerDelegate.getIsCancelledInvocationCount());

		Assert.assertEquals(innerDelegate.isDoneExpected(), objectUnderTest.isDone());
		Assert.assertEquals(1, innerDelegate.getIsDoneInvocationCount());

		innerDelegate.setIsDoneExpected(!innerDelegate.isDoneExpected());
		Assert.assertEquals(innerDelegate.isDoneExpected(), objectUnderTest.isDone());
		Assert.assertEquals(2, innerDelegate.getIsDoneInvocationCount());
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


	/**
	 * Implementation of {@link Future} for the unit tests in this class.
	 */
	static class TestFuture implements Future<Integer> {

		private boolean cancelExpected = false;
		private boolean isCancelledExpected = false;
		private boolean isDoneExpected = false;

		private int cancelInvocationCount = 0;
		private int isCancelledInvocationCount = 0;
		private int isDoneInvocationCount = 0;
		private int getInvocationCount = 0;
		private int getTimeoutInvocationCount = 0;

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			++cancelInvocationCount;
			return cancelExpected;
		}

		@Override
		public boolean isCancelled() {
			++isCancelledInvocationCount;
			return isCancelledExpected;
		}

		@Override
		public boolean isDone() {
			++isDoneInvocationCount;
			return isDoneExpected;
		}

		@Override
		public Integer get() {
			++getInvocationCount;
			return 4711;
		}

		@Override
		public Integer get(long timeout, TimeUnit unit) {
			++getTimeoutInvocationCount;
			return 4711;
		}

		boolean isCancelExpected() {
			return cancelExpected;
		}

		boolean isCancelledExpected() {
			return isCancelledExpected;
		}

		boolean isDoneExpected() {
			return isDoneExpected;
		}

		void setCancelResult(boolean resultCancel) {
			this.cancelExpected = resultCancel;
		}

		void setIsCancelledResult(boolean resultIsCancelled) {
			this.isCancelledExpected = resultIsCancelled;
		}

		void setIsDoneExpected(boolean resultIsDone) {
			this.isDoneExpected = resultIsDone;
		}

		int getCancelInvocationCount() {
			return cancelInvocationCount;
		}

		int getIsCancelledInvocationCount() {
			return isCancelledInvocationCount;
		}

		int getIsDoneInvocationCount() {
			return isDoneInvocationCount;
		}

		int getGetInvocationCount() {
			return getInvocationCount;
		}

		int getGetTimeoutInvocationCount() {
			return getTimeoutInvocationCount;
		}
	}
}
