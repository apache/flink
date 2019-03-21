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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.util.MurmurHashUtil;

import static org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET;

/**
 * Util for hash code of data format.
 */
public class HashUtil {

	public static int hashInt(int value) {
		return Integer.hashCode(value);
	}

	public static int hashLong(long value) {
		return Long.hashCode(value);
	}

	public static int hashShort(short value) {
		return Short.hashCode(value);
	}

	public static int hashByte(byte value) {
		return Byte.hashCode(value);
	}

	public static int hashFloat(float value) {
		return Float.hashCode(value);
	}

	public static int hashDouble(double value) {
		return Double.hashCode(value);
	}

	public static int hashBoolean(boolean value) {
		return Boolean.hashCode(value);
	}

	public static int hashChar(char value) {
		return Character.hashCode(value);
	}

	public static int hashObject(Object value) {
		return value.hashCode();
	}

	public static int hashString(BinaryString value) {
		return value.hashCode();
	}

	public static int hashDecimal(Decimal value) {
		return value.hashCode();
	}

	public static int hashBinary(byte[] value) {
		return MurmurHashUtil.hashUnsafeBytes(value, BYTE_ARRAY_BASE_OFFSET, value.length);
	}
}
