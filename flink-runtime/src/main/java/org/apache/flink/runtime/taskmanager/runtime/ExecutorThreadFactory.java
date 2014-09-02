/**
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

package org.apache.flink.runtime.taskmanager.runtime;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorThreadFactory implements ThreadFactory {
	
	public static final ExecutorThreadFactory INSTANCE = new ExecutorThreadFactory();

	private static final String THREAD_NAME = "Flink Executor Thread ";
	
	private final AtomicInteger threadNumber = new AtomicInteger(1);
	
	
	private ExecutorThreadFactory() {}
	
	
	public Thread newThread(Runnable target) {
		Thread t = new Thread(target, THREAD_NAME + threadNumber.getAndIncrement());
		t.setDaemon(true);
		return t;
	}
}
