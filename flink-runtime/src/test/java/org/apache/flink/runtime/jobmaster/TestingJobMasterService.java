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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Implementation of the {@link JobMasterService} for testing purposes.
 */
public class TestingJobMasterService implements JobMasterService {

	@Nonnull
	private final String address;

	@Nonnull
	private final Supplier<? extends CompletableFuture<Void>> closeAsyncSupplier;

	private final JobMasterGateway jobMasterGateway;

	public TestingJobMasterService(@Nonnull String address, @Nonnull Supplier<? extends CompletableFuture<Void>> closeAsyncSupplier) {
		this.address = address;
		this.closeAsyncSupplier = closeAsyncSupplier;

		jobMasterGateway = new TestingJobMasterGatewayBuilder().build();
	}

	public TestingJobMasterService() {
		this(
			"localhost",
			() -> CompletableFuture.completedFuture(null));
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
		return closeAsyncSupplier.get();
	}
}
