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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple3;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * {@link BlobStore} implementation for testing purposes.
 */
public class TestingBlobStore implements BlobStore {

	@Nonnull
	private final Function<Tuple3<File, JobID, BlobKey>, Boolean> putFunction;

	@Nonnull
	private final BiFunction<JobID, BlobKey, Boolean> deleteFunction;

	@Nonnull
	private final Function<JobID, Boolean> deleteAllFunction;

	@Nonnull
	private final Function<Tuple3<JobID, BlobKey, File>, Boolean> getFunction;

	public TestingBlobStore(@Nonnull Function<Tuple3<File, JobID, BlobKey>, Boolean> putFunction, @Nonnull BiFunction<JobID, BlobKey, Boolean> deleteFunction, @Nonnull Function<JobID, Boolean> deleteAllFunction, @Nonnull Function<Tuple3<JobID, BlobKey, File>, Boolean> getFunction) {
		this.putFunction = putFunction;
		this.deleteFunction = deleteFunction;
		this.deleteAllFunction = deleteAllFunction;
		this.getFunction = getFunction;
	}

	@Override
	public boolean put(File localFile, JobID jobId, BlobKey blobKey) {
		return putFunction.apply(Tuple3.of(localFile, jobId, blobKey));
	}

	@Override
	public boolean delete(JobID jobId, BlobKey blobKey) {
		return deleteFunction.apply(jobId, blobKey);
	}

	@Override
	public boolean deleteAll(JobID jobId) {
		return deleteAllFunction.apply(jobId);
	}

	@Override
	public boolean get(JobID jobId, BlobKey blobKey, File localFile) {
		return getFunction.apply(Tuple3.of(jobId, blobKey, localFile));
	}
}
