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

	/**
	 * Internal code to identify a job-unrelated reference via content hash as the key.
	 * <p>
	 * Note: previously, there was also <tt>NAME_ADDRESSABLE</tt> (code <tt>1</tt>) and
	 * <tt>JOB_ID_SCOPE</tt> (code <tt>2</tt>).
	 */
	static final byte CONTENT_NO_JOB = 0;

	/**
	 * Internal code to identify a job-related reference via content hash as the key.
	 * <p>
	 * Note: previously, there was also <tt>NAME_ADDRESSABLE</tt> (code <tt>1</tt>) and
	 * <tt>JOB_ID_SCOPE</tt> (code <tt>2</tt>).
	 */
	static final byte CONTENT_FOR_JOB = 3;

	// --------------------------------------------------------------------------------------------

	private BlobServerProtocol() {}
}
