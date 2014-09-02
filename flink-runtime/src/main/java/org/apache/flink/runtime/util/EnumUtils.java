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


package org.apache.flink.runtime.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.flink.core.io.StringRecord;

/**
 * Auxiliary class to (de)serialize enumeration values.
 * 
 */
public final class EnumUtils {

	/**
	 * Private constructor to overwrite public one.
	 */
	private EnumUtils() {
	}

	/**
	 * Reads a value from the given enumeration from the specified input stream.
	 * 
	 * @param <T>
	 *        the type of the enumeration
	 * @param in
	 *        the input stream to read from
	 * @param enumType
	 *        the class of the enumeration
	 * @return the value of the given enumeration read from the input stream
	 * @throws IOException
	 *         thrown if any error occurred while reading data from the stream
	 */
	public static <T extends Enum<T>> T readEnum(final DataInput in, final Class<T> enumType) throws IOException {

		if (!in.readBoolean()) {
			return null;
		}

		return T.valueOf(enumType, StringRecord.readString(in));
	}

	/**
	 * Writes a value of an enumeration to the given output stream.
	 * 
	 * @param out
	 *        the output stream to write to
	 * @param enumVal
	 *        the value of a enumeration to be written to the output stream
	 * @throws IOException
	 *         thrown if any error occurred while writing data to the stream
	 */
	public static void writeEnum(final DataOutput out, final Enum<?> enumVal) throws IOException {

		if (enumVal == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			StringRecord.writeString(out, enumVal.name());
		}
	}
}
