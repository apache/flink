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

public class BlobServerProtocol {

	// --------------------------------------------------------------------------------------------
	//  Constants used in the protocol of the BLOB store
	// --------------------------------------------------------------------------------------------

	/** The buffer size in bytes for network transfers. */
	static final int BUFFER_SIZE = 65536; // 64 K

	/** The maximum key length allowed for storing BLOBs. */
	static final int MAX_KEY_LENGTH = 64;

	/** Internal code to identify a PUT operation. */
	static final byte PUT_OPERATION = 0;

	/** Internal code to identify a GET operation. */
	static final byte GET_OPERATION = 1;

	/** Internal code to identify a DELETE operation. */
	static final byte DELETE_OPERATION = 2;

	/** Internal code to identify a successful operation. */
	static final byte RETURN_OKAY = 0;

	/** Internal code to identify an erroneous operation. */
	static final byte RETURN_ERROR = 1;

	/** Internal code to identify a reference via content hash as the key */
	static final byte CONTENT_ADDRESSABLE = 0;

	/** Internal code to identify a reference via jobId and name as the key */
	static final byte NAME_ADDRESSABLE = 1;

	/** Internal code to identify a reference via jobId as the key */
	static final byte JOB_ID_SCOPE = 2;

	// --------------------------------------------------------------------------------------------

	private BlobServerProtocol() {}
}
