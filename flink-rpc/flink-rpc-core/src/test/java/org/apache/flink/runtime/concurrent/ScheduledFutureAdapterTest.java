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

import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** Unit tests for {@link ScheduledFutureAdapter}. */
@ExtendWith(TestLoggerExtension.class)
public class ScheduledFutureAdapterTest {

    private ScheduledFutureAdapter<Integer> objectUnderTest;
    private TestFuture innerDelegate;

    @BeforeEach
    public void before() {
        this.innerDelegate = new TestFuture();
        this.objectUnderTest =
                new ScheduledFutureAdapter<>(innerDelegate, 4200000321L, TimeUnit.NANOSECONDS);
    }

    @Test
    public void testForwardedMethods() throws Exception {

        assertEquals((Integer) 4711, objectUnderTest.get());
        assertEquals(1, innerDelegate.getGetInvocationCount());

        assertEquals((Integer) 4711, objectUnderTest.get(42L, TimeUnit.SECONDS));
        assertEquals(1, innerDelegate.getGetTimeoutInvocationCount());

        assertEquals(innerDelegate.isCancelExpected(), objectUnderTest.cancel(true));
        assertEquals(1, innerDelegate.getCancelInvocationCount());

        innerDelegate.setCancelResult(!innerDelegate.isCancelExpected());
        assertEquals(innerDelegate.isCancelExpected(), objectUnderTest.cancel(true));
        assertEquals(2, innerDelegate.getCancelInvocationCount());

        assertEquals(innerDelegate.isCancelledExpected(), objectUnderTest.isCancelled());
        assertEquals(1, innerDelegate.getIsCancelledInvocationCount());

        innerDelegate.setIsCancelledResult(!innerDelegate.isCancelledExpected());
        assertEquals(innerDelegate.isCancelledExpected(), objectUnderTest.isCancelled());
        assertEquals(2, innerDelegate.getIsCancelledInvocationCount());

        assertEquals(innerDelegate.isDoneExpected(), objectUnderTest.isDone());
        assertEquals(1, innerDelegate.getIsDoneInvocationCount());

        innerDelegate.setIsDoneExpected(!innerDelegate.isDoneExpected());
        assertEquals(innerDelegate.isDoneExpected(), objectUnderTest.isDone());
        assertEquals(2, innerDelegate.getIsDoneInvocationCount());
    }

    @Test
    public void testCompareToEqualsHashCode() {

        assertEquals(0, objectUnderTest.compareTo(objectUnderTest));
        assertEquals(objectUnderTest, objectUnderTest);

        ScheduledFutureAdapter<?> other =
                getDeepCopyWithAdjustedTime(0L, objectUnderTest.getTieBreakerUid());

        assertEquals(0, objectUnderTest.compareTo(other));
        assertEquals(0, other.compareTo(objectUnderTest));
        assertEquals(objectUnderTest, other);
        assertEquals(objectUnderTest.hashCode(), other.hashCode());

        other = getDeepCopyWithAdjustedTime(0L, objectUnderTest.getTieBreakerUid() + 1L);
        assertEquals(-1, Integer.signum(objectUnderTest.compareTo(other)));
        assertEquals(+1, Integer.signum(other.compareTo(objectUnderTest)));
        assertNotEquals(objectUnderTest, other);

        other = getDeepCopyWithAdjustedTime(+1L, objectUnderTest.getTieBreakerUid());
        assertEquals(-1, Integer.signum(objectUnderTest.compareTo(other)));
        assertEquals(+1, Integer.signum(other.compareTo(objectUnderTest)));
        assertNotEquals(objectUnderTest, other);

        other = getDeepCopyWithAdjustedTime(-1L, objectUnderTest.getTieBreakerUid());
        assertEquals(+1, Integer.signum(objectUnderTest.compareTo(other)));
        assertEquals(-1, Integer.signum(other.compareTo(objectUnderTest)));
        assertNotEquals(objectUnderTest, other);
    }

    private ScheduledFutureAdapter<Integer> getDeepCopyWithAdjustedTime(long nanoAdjust, long uid) {
        return new ScheduledFutureAdapter<>(
                innerDelegate, objectUnderTest.getScheduleTimeNanos() + nanoAdjust, uid);
    }

    /** Implementation of {@link Future} for the unit tests in this class. */
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
        public Integer get(long timeout, @Nonnull TimeUnit unit) {
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
