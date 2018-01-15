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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.AbstractID;

import javax.xml.bind.DatatypeConverter;

import java.nio.ByteBuffer;

/**
 * Unique (at least statistically unique) identifier for a Flink Job. Jobs in Flink correspond
 * do dataflow graphs.
 * 
 * <p>Jobs act simultaneously as <i>sessions</i>, because jobs can be created and submitted
 * incrementally in different parts. Newer fragments of a graph can be attached to existing
 * graphs, thereby extending the current data flow graphs.</p>
 */
@Public
public final class JobID extends AbstractID {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new (statistically) random JobID.
	 */
	public JobID() {
		super();
	}

	/**
	 * Creates a new JobID, using the given lower and upper parts.
	 * 
	 * @param lowerPart The lower 8 bytes of the ID. 
	 * @param upperPart The upper 8 bytes of the ID.
	 */
	public JobID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	/**
	 * Creates a new JobID from the given byte sequence. The byte sequence must be
	 * exactly 16 bytes long. The first eight bytes make up the lower part of the ID,
	 * while the next 8 bytes make up the upper part of the ID.
	 *
	 * @param bytes The byte sequence.
	 */
	public JobID(byte[] bytes) {
		super(bytes);
	}
	
	// ------------------------------------------------------------------------
	//  Static factory methods
	// ------------------------------------------------------------------------

	/**
	 * Creates a new (statistically) random JobID.
	 * 
	 * @return A new random JobID.
	 */
	public static JobID generate() {
		return new JobID();
	}

	/**
	 * Creates a new JobID from the given byte sequence. The byte sequence must be
	 * exactly 16 bytes long. The first eight bytes make up the lower part of the ID,
	 * while the next 8 bytes make up the upper part of the ID.
	 * 
	 * @param bytes The byte sequence.
	 *                 
	 * @return A new JobID corresponding to the ID encoded in the bytes.
	 */
	public static JobID fromByteArray(byte[] bytes) {
		return new JobID(bytes);
	}

	public static JobID fromByteBuffer(ByteBuffer buf) {
		long lower = buf.getLong();
		long upper = buf.getLong();
		return new JobID(lower, upper);
	}

	/**
	 * Parses a JobID from the given string.
	 *
	 * @param hexString string representation of a JobID
	 * @return Parsed JobID
	 * @throws IllegalArgumentException if the JobID could not be parsed from the given string
	 */
	public static JobID fromHexString(String hexString) {
		try {
			return new JobID(DatatypeConverter.parseHexBinary(hexString));
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot parse JobID from \"" + hexString + "\".", e);
		}
	}
}
