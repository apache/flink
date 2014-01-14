/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

import java.nio.ByteBuffer;

import javax.xml.bind.DatatypeConverter;

import eu.stratosphere.nephele.AbstractID;

public final class JobID extends AbstractID {

	public JobID() {
		super();
	}

	public JobID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public JobID(byte[] bytes) {
		super(bytes);
	}

	public static JobID generate() {
		long lowerPart = AbstractID.generateRandomLong();
		long upperPart = AbstractID.generateRandomLong();

		return new JobID(lowerPart, upperPart);
	}

	public static JobID fromByteArray(byte[] bytes) {
		return new JobID(bytes);
	}

	public static JobID fromByteBuffer(ByteBuffer buf, int offset) {
		long lower = buf.getLong(offset);
		long upper = buf.getLong(offset + 8);
		return new JobID(lower, upper);
	}

	public static JobID fromHexString(String hexString) {
		return new JobID(DatatypeConverter.parseHexBinary(hexString));
	}
}
