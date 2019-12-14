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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/**
 * A common timer service interface with life cycle methods.
 *
 * <p>The registration of timers follows a life cycle of three phases:
 * <ol>
 *     <li>In the initial state, it accepts timer registrations and triggers when the time is reached.</li>
 *     <li>After calling {@link #quiesce()}, further calls to
 *         {@link #registerTimer(long, ProcessingTimeCallback)} will not register any further timers, and will
 *         return a "dummy" future as a result. This is used for clean shutdown, where currently firing
 *         timers are waited for and no future timers can be scheduled, without causing hard exceptions.</li>
 *     <li>After a call to {@link #shutdownService()}, all calls to {@link #registerTimer(long, ProcessingTimeCallback)}
 *         will result in a hard exception.</li>
 * </ol>
 */
@Internal
public interface TimerService extends ProcessingTimeService {

	/**
	 * Returns <tt>true</tt> if the service has been shut down, <tt>false</tt> otherwise.
	 */
	boolean isTerminated();

	/**
	 * This method puts the service into a state where it does not register new timers, but
	 * returns for each call to {@link #registerTimer(long, ProcessingTimeCallback)} only a "mock" future.
	 * Furthermore, the method clears all not yet started timers.
	 *
	 * <p>This method can be used to cleanly shut down the timer service. The using components
	 * will not notice that the service is shut down (as for example via exceptions when registering
	 * a new timer), but the service will simply not fire any timer any more.
	 */
	void quiesce() throws InterruptedException;

	/**
	 * This method can be used after calling {@link #quiesce()}, and awaits the completion
	 * of currently executing timers.
	 */
	void awaitPendingAfterQuiesce() throws InterruptedException;

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception.
	 */
	void shutdownService();

	/**
	 * Shuts down and clean up the timer service provider hard and immediately. This does not wait
	 * for any timer to complete. Any further call to {@link #registerTimer(long, ProcessingTimeCallback)}
	 * will result in a hard exception. This call cannot be interrupted and will block until the shutdown is completed
	 * or the timeout is exceeded.
	 *
	 * @param timeoutMs timeout for blocking on the service shutdown in milliseconds.
	 * @return returns true iff the shutdown was completed.
	 */
	boolean shutdownServiceUninterruptible(long timeoutMs);
}
