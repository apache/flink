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

package org.apache.flink.testutils.executor;

import org.junit.rules.ExternalResource;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Resource which starts/stops an {@link ExecutorService} for testing purposes.
 */
public class TestExecutorResource extends ExternalResource {

	private final Supplier<ExecutorService> serviceFactory;

	private ExecutorService executorService;

	public TestExecutorResource(Supplier<ExecutorService> serviceFactory) {
		this.serviceFactory = serviceFactory;
	}

	@Override
	protected void before() throws Throwable {
		executorService = serviceFactory.get();
	}

	public Executor getExecutor() {
		// only return an Executor since this resource is in charge of the life cycle
		return executorService;
	}

	@Override
	protected void after() {
		if (executorService != null) {
			executorService.shutdown();
		}
	}
}
