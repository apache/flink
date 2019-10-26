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

import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.util.function.TriFunction;

import java.util.Collection;

class TestingDispatcherServiceFactory implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {
	private final TriFunction<DispatcherId, Collection<JobGraph>, JobGraphWriter, AbstractDispatcherLeaderProcess.DispatcherGatewayService> createFunction;

	private TestingDispatcherServiceFactory(TriFunction<DispatcherId, Collection<JobGraph>, JobGraphWriter, AbstractDispatcherLeaderProcess.DispatcherGatewayService> createFunction) {
		this.createFunction = createFunction;
	}

	@Override
	public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {
		return createFunction.apply(fencingToken, recoveredJobs, jobGraphWriter);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public static class Builder {
		private TriFunction<DispatcherId, Collection<JobGraph>, JobGraphWriter, AbstractDispatcherLeaderProcess.DispatcherGatewayService> createFunction = (ignoredA, ignoredB, ignoredC) -> TestingDispatcherGatewayService.newBuilder().build();

		private Builder() {}

		Builder setCreateFunction(TriFunction<DispatcherId, Collection<JobGraph>, JobGraphWriter, AbstractDispatcherLeaderProcess.DispatcherGatewayService> createFunction) {
			this.createFunction = createFunction;
			return this;
		}

		public TestingDispatcherServiceFactory build() {
			return new TestingDispatcherServiceFactory(createFunction);
		}
	}
}
