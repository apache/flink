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

import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The BLOB cache provides access to BLOB services for permanent and transient BLOBs.
 */
public final class BlobCache implements BlobService {

	/** Caching store for permanent BLOBs. */
	private final PermanentBlobCache permanentBlobStore;

	/** Store for transient BLOB files. */
	private final TransientBlobCache transientBlobStore;

	/**
	 * Instantiates a new BLOB cache.
	 *
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from
	 * @param blobClientConfig
	 * 		global configuration
	 * @param blobView
	 * 		(distributed) blob store file system to retrieve files from first
	 *
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public BlobCache(
			final InetSocketAddress serverAddress,
			final Configuration blobClientConfig,
			final BlobView blobView) throws IOException {

		this.permanentBlobStore = new PermanentBlobCache(serverAddress, blobClientConfig, blobView);
		this.transientBlobStore = new TransientBlobCache(serverAddress, blobClientConfig);
	}

	@Override
	public PermanentBlobCache getPermanentBlobStore() {
		return permanentBlobStore;
	}

	@Override
	public TransientBlobCache getTransientBlobStore() {
		return transientBlobStore;
	}

	@Override
	public void close() throws IOException {
		permanentBlobStore.close();
		transientBlobStore.close();
	}

	@Override
	public int getPort() {
		// NOTE: both blob stores connect to the same server!
		return permanentBlobStore.getPort();
	}
}
