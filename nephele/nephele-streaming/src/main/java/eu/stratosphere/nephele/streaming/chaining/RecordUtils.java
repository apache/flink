/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming.chaining;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class RecordUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private RecordUtils() {
	}

	/**
	 * Creates a copy of the given {@link Record} object by an in-memory serialization and subsequent
	 * deserialization.
	 * 
	 * @param original
	 *        the original object to be copied
	 * @return the copy of original object created by the original object's serialization/deserialization methods
	 * @throws IOException
	 *         thrown if an error occurs while copying the object
	 */
	public static Record createCopy(final Record original) throws IOException {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		original.write(dos);

		Record copy;
		try {
			copy = original.getClass().newInstance();
		} catch (Exception e) {
			throw new IOException(StringUtils.stringifyException(e));
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final DataInputStream dis = new DataInputStream(bais);

		copy.read(dis);

		return copy;
	}
}
