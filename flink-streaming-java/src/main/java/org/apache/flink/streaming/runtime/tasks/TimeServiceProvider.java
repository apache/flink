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

import java.util.concurrent.ScheduledFuture;

/**
 * Defines the current processing time and handles all related actions,
 * such as register timers for tasks to be executed in the future.
 */
public abstract class TimeServiceProvider {

	/** Returns the current processing time. */
	public abstract long getCurrentProcessingTime();

	/** Registers a task to be executed when (processing) time is {@code timestamp}.
	 * @param timestamp
	 * 						when the task is to be executed (in processing time)
	 * @param target
	 * 						the task to be executed
	 * @return the result to be returned.
	 */
	public abstract ScheduledFuture<?> registerTimer(final long timestamp, final Runnable target);

	/** Shuts down and clean up the timer service provider. */
	public abstract void shutdownService() throws Exception;
}
