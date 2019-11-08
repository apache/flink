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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.ExecutorUtils;

import org.junit.rules.ExternalResource;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Provide an automatically shut down scheduled executor for testing.
 */
public class TestingScheduledExecutor extends ExternalResource {

	private long shutdownTimeoutMillis;
	private ScheduledExecutor scheduledExecutor;
	private ScheduledExecutorService innerExecutorService;

	public TestingScheduledExecutor() {
		this(500L);
	}

	public TestingScheduledExecutor(long shutdownTimeoutMillis) {
		this.shutdownTimeoutMillis = shutdownTimeoutMillis;
	}

	@Override
	public void before() {
		this.innerExecutorService = Executors.newSingleThreadScheduledExecutor();
		this.scheduledExecutor = new ScheduledExecutorServiceAdapter(innerExecutorService);
	}

	@Override
	protected void after() {
		ExecutorUtils.gracefulShutdown(shutdownTimeoutMillis, TimeUnit.MILLISECONDS, innerExecutorService);
	}

	protected ScheduledExecutor getScheduledExecutor() {
		return scheduledExecutor;
	}
}
