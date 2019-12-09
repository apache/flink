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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Implementation of the {@link JobMasterService} for testing purposes.
 */
public class TestingJobMasterService implements JobMasterService {

	@Nonnull
	private final String address;

	@Nonnull
	private final Function<Exception, CompletableFuture<Acknowledge>> suspendFunction;

	@Nonnull
	private final Function<JobMasterId, CompletableFuture<Acknowledge>> startFunction;

	private JobMasterGateway jobMasterGateway;

	public TestingJobMasterService(@Nonnull String address, @Nonnull Function<Exception, CompletableFuture<Acknowledge>> suspendFunction) {
		this(address, suspendFunction, ignored -> CompletableFuture.completedFuture(Acknowledge.get()));
	}

	public TestingJobMasterService(
		@Nonnull String address,
		@Nonnull Function<Exception, CompletableFuture<Acknowledge>> suspendFunction,
		@Nonnull Function<JobMasterId, CompletableFuture<Acknowledge>> startFunction) {
		this.address = address;
		this.suspendFunction = suspendFunction;
		this.startFunction = startFunction;
	}

	public TestingJobMasterService() {
		this(
			"localhost",
			e -> CompletableFuture.completedFuture(Acknowledge.get()));
	}

	@Override
	public CompletableFuture<Acknowledge> start(JobMasterId jobMasterId) {
			jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
			return startFunction.apply(jobMasterId);
	}

	@Override
	public CompletableFuture<Acknowledge> suspend(Exception cause) {
		jobMasterGateway = null;
		return suspendFunction.apply(cause);
	}

	@Override
	public JobMasterGateway getGateway() {
		Preconditions.checkNotNull(jobMasterGateway, "TestingJobMasterService has not been started yet.");
		return jobMasterGateway;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		jobMasterGateway = null;
		return CompletableFuture.completedFuture(null);
	}
}
