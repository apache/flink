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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobServer;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Specialisation of {@link BlobLibraryCacheManager} that works with a {@link BlobServer}.
 */
public final class BlobServerLibraryManager extends BlobLibraryCacheManager {

	/** The blob service to download libraries */
	private final BlobServer blobServer;

	public BlobServerLibraryManager(BlobServer blobService) {
		super();
		this.blobServer = checkNotNull(blobService);
	}

	@Override
	public BlobServer getBlobService() {
		return blobServer;
	}

	protected void registerJobWithBlobService(@Nonnull JobID jobId) {
		// nothing to do here
	}

	@Override
	public void unregisterJob(@Nonnull JobID jobId) {
		checkNotNull(jobId, "The JobId must not be null.");
		// nothing to do here
	}
}
