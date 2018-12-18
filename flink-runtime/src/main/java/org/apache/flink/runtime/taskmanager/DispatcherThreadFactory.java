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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadFactory;

/**
 * Thread factory that creates threads with a given name, associates them with a given
 * thread group, and set them to daemon mode.
 */
public class DispatcherThreadFactory implements ThreadFactory {

	private final ThreadGroup group;

	private final String threadName;

	@Nullable
	private final ClassLoader classLoader;

	/**
	 * Creates a new thread factory.
	 *
	 * @param group The group that the threads will be associated with.
	 * @param threadName The name for the threads.
	 */
	public DispatcherThreadFactory(ThreadGroup group, String threadName) {
		this(group, threadName, null);
	}

	/**
	 * Creates a new thread factory.
	 *
	 * @param group The group that the threads will be associated with.
	 * @param threadName The name for the threads.
	 * @param classLoader The {@link ClassLoader} to be set as context class loader.
	 */
	public DispatcherThreadFactory(
			ThreadGroup group,
			String threadName,
			@Nullable ClassLoader classLoader) {
		this.group = group;
		this.threadName = threadName;
		this.classLoader = classLoader;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, threadName);
		if (classLoader != null) {
			t.setContextClassLoader(classLoader);
		}
		t.setDaemon(true);
		t.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
		return t;
	}
}
