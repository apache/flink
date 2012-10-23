/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.util;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Auxiliary class to (de)serialize enumeration values.
 * 
 * @author warneke
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
	 */
	public static <T extends Enum<T>> T readEnum(final Input in, final Class<T> enumType) {

		if (!in.readBoolean()) {
			return null;
		}

		return T.valueOf(enumType, in.readString());
	}

	/**
	 * Writes a value of an enumeration to the given output stream.
	 * 
	 * @param out
	 *        the output stream to write to
	 * @param enumVal
	 *        the value of a enumeration to be written to the output stream
	 */
	public static void writeEnum(final Output out, final Enum<?> enumVal) {

		if (enumVal == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeString(enumVal.name());
		}
	}
}
