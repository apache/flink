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

package org.apache.flink.docs.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * No-op implementation of {@link TransientBlobService} used by the {@link RestAPIDocGenerator}.
 */
enum NoOpTransientBlobService implements TransientBlobService {
	INSTANCE;

	@Override
	public File getFile(TransientBlobKey key) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TransientBlobKey putTransient(byte[] value) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean deleteFromCache(TransientBlobKey key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {}
}
