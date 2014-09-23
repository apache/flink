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

package org.apache.flink.runtime.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorThreadFactory implements ThreadFactory {
	
	private static final Logger LOG = LoggerFactory.getLogger(ExecutorThreadFactory.class);
	
	
	private static final String THREAD_NAME_PREFIX = "Flink Executor Thread - ";
	
	private static final AtomicInteger COUNTER = new AtomicInteger(1);
	
	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("Flink Executor Threads");
	
	private static final Thread.UncaughtExceptionHandler EXCEPTION_HANDLER = new LoggingExceptionHander();
	
	
	public static final ExecutorThreadFactory INSTANCE = new ExecutorThreadFactory();
	
	// --------------------------------------------------------------------------------------------
	
	private ExecutorThreadFactory() {}
	
	
	public Thread newThread(Runnable target) {
		Thread t = new Thread(THREAD_GROUP, target, THREAD_NAME_PREFIX + COUNTER.getAndIncrement());
		t.setDaemon(true);
		t.setUncaughtExceptionHandler(EXCEPTION_HANDLER);
		return t;
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class LoggingExceptionHander implements Thread.UncaughtExceptionHandler {

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Thread '" + t.getName() + "' produced an uncaught exception.", e);
			}
		}
	}
}
