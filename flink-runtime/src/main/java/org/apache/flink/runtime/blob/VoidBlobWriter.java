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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;

import java.io.IOException;
import java.io.InputStream;

/**
 * BlobWriter which does not support writing BLOBs to a store. This class is
 * mainly used for testing purposes where we don't want to store data in the
 * BLOB store.
 */
@VisibleForTesting
public class VoidBlobWriter implements BlobWriter {

	private static final VoidBlobWriter INSTANCE = new VoidBlobWriter();

	@Override
	public PermanentBlobKey putPermanent(JobID jobId, byte[] value) throws IOException {
		throw new IOException("The VoidBlobWriter cannot write data to the BLOB store.");
	}

	@Override
	public PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream) throws IOException {
		throw new IOException("The VoidBlobWriter cannot write data to the BLOB store.");
	}

	@Override
	public int getMinOffloadingSize() {
		return Integer.MAX_VALUE;
	}

	public static VoidBlobWriter getInstance() {
		return INSTANCE;
	}
}
