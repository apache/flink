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

package org.apache.flink.connectors.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.runtime.conversion.DataStructureConverters;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A utility class to process data exchange with HBase type system.
 */
@Internal
public class HBaseTypeUtils {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private static final byte[] EMPTY_BYTES = new byte[]{};

	//convert to Internal type from HBase bytes
	public static Object deserializeToInnerObject(byte[] value, int typeIdx, String stringCharset) throws UnsupportedEncodingException {
		switch (typeIdx) {
			case 0: // byte[]
				return value;
			case 1: //BinaryString
				return fromBytes(value, stringCharset);
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
				return DataStructureConverters.getConverterForType(DataTypes.TIMESTAMP).toInternal(new Timestamp(Bytes.toLong(value)));
			case 10: // sql.Date encoded as long
				return DataStructureConverters.getConverterForType(DataTypes.DATE).toInternal(new Date(Bytes.toLong(value)));
			case 11: // sql.Time encoded as long
				return DataStructureConverters.getConverterForType(DataTypes.TIME).toInternal(new Time(Bytes.toLong(value)));
			case 12:
				return fromBytesToDecimal(value);
			case 13:
				return new BigInteger(value);

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	//convert to external Java type from HBase bytes
	public static Object deserializeToObject(byte[] value, int typeIdx, String stringCharset) throws UnsupportedEncodingException {
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
				return fromBytesToDecimal(value);
			case 13:
				return new BigInteger(value);

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	//convert to HBase bytes from the internal type
	public static byte[] serializeFromInternalObject(Object value, int typeIdx, String stringCharset) throws UnsupportedEncodingException {
		switch (typeIdx) {
			case 0: // byte[]
				return (byte[]) value;
			case 1: //binaryString
				return value == null ? EMPTY_BYTES : toBytes((BinaryString) value, stringCharset);
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
			case 9: // sql.Timestamp encoded as long
				return Bytes.toBytes(((Timestamp) (DataStructureConverters.getConverterForType(DataTypes.TIMESTAMP).toExternalImpl(value))).getTime());
			case 10: // sql.Date encoded as long
				return Bytes.toBytes(((Date) (DataStructureConverters.getConverterForType(DataTypes.TIMESTAMP).toExternalImpl(value))).getTime());
			case 11: // sql.Time encoded as long
				return Bytes.toBytes(((Time) (DataStructureConverters.getConverterForType(DataTypes.TIMESTAMP).toExternalImpl(value))).getTime());
			case 12: //flink decimal
				return decimalToBytes((Decimal) value);
			case 13:
				return ((BigInteger) value).toByteArray();

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	//convert to HBase bytes from the external type
	public static byte[] serializeFromObject(Object value, int typeIdx, String stringCharset) throws UnsupportedEncodingException {
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
				return Bytes.toBytes(((TimeStamp) value).getTime());
			case 10: // sql.Date encoded as long
				return Bytes.toBytes(((Date) value).getTime());
			case 11: // sql.Time encoded as long
				return Bytes.toBytes(((Time) value).getTime());
			case 12: // decimal
				return decimalToBytes((Decimal) value);
			case 13:
				return ((BigInteger) value).toByteArray();

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	public static int getTypeIndex(Class<?> clazz) {
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
			throw new IllegalArgumentException("unsupported type class:" + clazz);
		}
	}

	public static int getTypeIndex(TypeInformation typeInfo) {
		return getTypeIndex(typeInfo.getTypeClass());
	}

	public static boolean isSupportedType(Class<?> clazz) {
		return getTypeIndex(clazz) != -1;
	}

	public static boolean isSupportedType(TypeInformation typeInfo) {
		return getTypeIndex(typeInfo) != -1;
	}

	// BinaryString
	public static byte[] toBytes(BinaryString string, String stringCharset) throws UnsupportedEncodingException {
		return string.toString().getBytes(stringCharset);
	}

	public static byte[] toBytes(BinaryString string) throws UnsupportedEncodingException {
		return string.toString().getBytes(DEFAULT_CHARSET);
	}

	public static BinaryString fromBytes(byte[] bytes, String stringCharset) throws UnsupportedEncodingException {
		return BinaryString.fromString(new String(bytes, stringCharset));
	}

	public static BinaryString fromBytes(byte[] bytes) throws UnsupportedEncodingException {
		return BinaryString.fromString(new String(bytes, DEFAULT_CHARSET));
	}

	// BigDecimal
	public static byte[] decimalToBytes(Decimal decimal) {
		return Bytes.toBytes(decimal.toBigDecimal());
	}

	public static Decimal fromBytesToDecimal(byte[] bytes) {
		return fromBytesToDecimal(bytes, DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE);
	}

	public static Decimal fromBytesToDecimal(byte[] bytes, int precision, int scale) {
		return Decimal.fromBigDecimal(Bytes.toBigDecimal(bytes), precision, scale);
	}
}
