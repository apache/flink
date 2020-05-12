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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A utility class to process data exchange with HBase type system.
 */
@Internal
public class HBaseTypeUtils {

	private static final byte[] EMPTY_BYTES = new byte[]{};

	/**
	 * Deserialize byte array to Java Object with the given type.
	 */
	public static Object deserializeToObject(byte[] value, int typeIdx, Charset stringCharset) {
		switch (typeIdx) {
			case 0: // byte[]
				return value;
			case 1: // String
				return new String(value, stringCharset);
			case 2: // byte
				return value[0];
			case 3:
				return Bytes.toShort(value);
			case 4:
				return Bytes.toInt(value);
			case 5:
				return Bytes.toLong(value);
			case 6:
				return Bytes.toFloat(value);
			case 7:
				return Bytes.toDouble(value);
			case 8:
				return Bytes.toBoolean(value);
			case 9: // sql.Timestamp encoded as long
				return new Timestamp(Bytes.toLong(value));
			case 10: // sql.Date encoded as long
				return new Date(Bytes.toLong(value));
			case 11: // sql.Time encoded as long
				return new Time(Bytes.toLong(value));
			case 12:
				return Bytes.toBigDecimal(value);
			case 13:
				return new BigInteger(value);

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	/**
	 * Serialize the Java Object to byte array with the given type.
	 */
	public static byte[] serializeFromObject(Object value, int typeIdx, Charset stringCharset) {
		switch (typeIdx) {
			case 0: // byte[]
				return (byte[]) value;
			case 1: // external String
				return value == null ? EMPTY_BYTES : ((String) value).getBytes(stringCharset);
			case 2: // byte
				return value == null ? EMPTY_BYTES : new byte[]{(byte) value};
			case 3:
				return Bytes.toBytes((short) value);
			case 4:
				return Bytes.toBytes((int) value);
			case 5:
				return Bytes.toBytes((long) value);
			case 6:
				return Bytes.toBytes((float) value);
			case 7:
				return Bytes.toBytes((double) value);
			case 8:
				return Bytes.toBytes((boolean) value);
			case 9: // sql.Timestamp encoded to Long
				return Bytes.toBytes(((Timestamp) value).getTime());
			case 10: // sql.Date encoded as long
				return Bytes.toBytes(((Date) value).getTime());
			case 11: // sql.Time encoded as long
				return Bytes.toBytes(((Time) value).getTime());
			case 12:
				return Bytes.toBytes((BigDecimal) value);
			case 13:
				return ((BigInteger) value).toByteArray();

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	/**
	 * Gets the type index (type representation in HBase connector) from the {@link TypeInformation}.
	 */
	public static int getTypeIndex(TypeInformation typeInfo) {
		return getTypeIndex(typeInfo.getTypeClass());
	}

	/**
	 * Checks whether the given Class is a supported type in HBase connector.
	 */
	public static boolean isSupportedType(Class<?> clazz) {
		return getTypeIndex(clazz) != -1;
	}

	private static int getTypeIndex(Class<?> clazz) {
		if (byte[].class.equals(clazz)) {
			return 0;
		} else if (String.class.equals(clazz)) {
			return 1;
		} else if (Byte.class.equals(clazz)) {
			return 2;
		} else if (Short.class.equals(clazz)) {
			return 3;
		} else if (Integer.class.equals(clazz)) {
			return 4;
		} else if (Long.class.equals(clazz)) {
			return 5;
		} else if (Float.class.equals(clazz)) {
			return 6;
		} else if (Double.class.equals(clazz)) {
			return 7;
		} else if (Boolean.class.equals(clazz)) {
			return 8;
		} else if (Timestamp.class.equals(clazz)) {
			return 9;
		} else if (Date.class.equals(clazz)) {
			return 10;
		} else if (Time.class.equals(clazz)) {
			return 11;
		} else if (BigDecimal.class.equals(clazz)) {
			return 12;
		} else if (BigInteger.class.equals(clazz)) {
			return 13;
		} else {
			return -1;
		}
	}
}
