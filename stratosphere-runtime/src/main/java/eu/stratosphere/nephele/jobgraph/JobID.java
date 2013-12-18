/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobgraph;

import javax.xml.bind.DatatypeConverter;

import eu.stratosphere.nephele.io.AbstractID;

/**
 * A class for statistically unique job IDs.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class JobID extends AbstractID {

	/**
	 * Constructs a new random ID from a uniform distribution.
	 */
	public JobID() {
		super();
	}

	/**
	 * Constructs a new job ID.
	 * 
	 * @param lowerPart
	 *        the lower bytes of the ID
	 * @param upperPart
	 *        the higher bytes of the ID
	 */
	private JobID(final long lowerPart, final long upperPart) {
		super(lowerPart, upperPart);
	}

	/**
	 * Constructs a new job ID from the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the job ID with
	 */
	public JobID(final byte[] bytes) {
		super(bytes);
	}

	/**
	 * Generates a new statistically unique job ID.
	 * 
	 * @return a new statistically unique job ID
	 */
	public static JobID generate() {

		final long lowerPart = AbstractID.generateRandomBytes();
		final long upperPart = AbstractID.generateRandomBytes();

		return new JobID(lowerPart, upperPart);
	}

	/**
	 * Constructs a new job ID and initializes it with the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the new job ID with
	 * @return the new job ID
	 */
	public static JobID fromByteArray(final byte[] bytes) {

		return new JobID(bytes);
	}
	
	/**
	 * Constructs a new job ID and initializes it with the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the new job ID with
	 * @return the new job ID
	 */
	public static JobID fromHexString(final String hexString) {

		return new JobID(DatatypeConverter.parseHexBinary(hexString));
	}
}