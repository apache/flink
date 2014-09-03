/**
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


package org.apache.flink.runtime.managementgraph;

import javax.xml.bind.DatatypeConverter;

import org.apache.flink.runtime.AbstractID;

/**
 * A management group vertex ID uniquely identifies a {@link ManagementGroupVertex}.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class ManagementGroupVertexID extends AbstractID {
	
	
	/**
	 * Constructs a new ManagementGroupVertexID
	 * 
	 */
	public ManagementGroupVertexID() {
		super();
	}
	
	/**
	 * Constructs a new ManagementGroupVertexID from the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the job ID with
	 */
	public ManagementGroupVertexID(final byte[] bytes) {
		super(bytes);
	}
	
	/**
	 * Constructs a new job ID and initializes it with the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the new job ID with
	 * @return the new job ID
	 */
	public static ManagementGroupVertexID fromHexString(final String hexString) {

		return new ManagementGroupVertexID(DatatypeConverter.parseHexBinary(hexString));
	}
	
}
