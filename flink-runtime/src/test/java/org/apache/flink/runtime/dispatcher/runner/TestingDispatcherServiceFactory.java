/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;

import java.util.Collection;
import java.util.function.BiFunction;

class TestingDispatcherServiceFactory implements DispatcherLeaderProcessImpl.DispatcherServiceFactory {
	private final BiFunction<Collection<JobGraph>, JobGraphStore, DispatcherLeaderProcessImpl.DispatcherService> createFunction;

	private TestingDispatcherServiceFactory(BiFunction<Collection<JobGraph>, JobGraphStore, DispatcherLeaderProcessImpl.DispatcherService> createFunction) {
		this.createFunction = createFunction;
	}

	@Override
	public DispatcherLeaderProcessImpl.DispatcherService create(Collection<JobGraph> recoveredJobs, JobGraphStore jobGraphStore) {
		return createFunction.apply(recoveredJobs, jobGraphStore);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private BiFunction<Collection<JobGraph>, JobGraphStore, DispatcherLeaderProcessImpl.DispatcherService> createFunction = (ignoredA, ignoredB) -> TestingDispatcherService.newBuilder().build();

		private Builder() {}

		Builder setCreateFunction(BiFunction<Collection<JobGraph>, JobGraphStore, DispatcherLeaderProcessImpl.DispatcherService> createFunction) {
			this.createFunction = createFunction;
			return this;
		}

		public TestingDispatcherServiceFactory build() {
			return new TestingDispatcherServiceFactory(createFunction);
		}
	}
}
