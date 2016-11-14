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

import java.io.IOException;
import java.net.URL;

/**
 * A simple store and retrieve binary large objects (BLOBs).
 */
public interface BlobService {

	/**
	 * This method returns the URL of the file associated with the provided blob key.
	 *
	 * @param key blob key associated with the requested file
	 * @return The URL to the file.
	 * @throws IOException
	 */
	URL getURL(BlobKey key) throws IOException;


	/**
	 * This method deletes the file associated with the provided blob key.
	 *
	 * @param key associated with the file to be deleted
	 * @throws IOException
	 */
	void delete(BlobKey key) throws IOException;

	/**
	 * Returns the port of the blob service.
	 * @return the port of the blob service.
	 */
	int getPort();

	/**
	 * Shutdown method which is called to terminate the blob service.
	 */
	void shutdown();
	
	BlobClient createClient() throws IOException;
}
