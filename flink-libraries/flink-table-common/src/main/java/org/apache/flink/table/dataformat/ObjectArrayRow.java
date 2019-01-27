/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.util.StringUtils;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * An row implementation that uses an array of objects as the underlying storage.
 */
public abstract class ObjectArrayRow implements BaseRow {

	private byte header;
	protected final Object[] fields;

	public ObjectArrayRow(int arity) {
		this.fields = new Object[arity];
	}

	@Override
	public int getArity() {
		return fields.length;
	}

	@Override
	public byte getHeader() {
		return header;
	}

	@Override
	public void setHeader(byte header) {
		this.header = header;
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return this.fields[ordinal] == null;
	}

	@Override
	public byte[] getByteArray(int ordinal) {
		return (byte[]) this.fields[ordinal];
	}

	@Override
	public BinaryString getBinaryString(int ordinal) {
		Object value = this.fields[ordinal];
		if (value instanceof BinaryString) {
			return (BinaryString) value;
		} else {
			return BinaryString.fromString((String) value);
		}
	}

	@Override
	public BinaryString getBinaryString(int ordinal, BinaryString reuse) {
		return getBinaryString(ordinal);
	}

	@Override
	public String getString(int ordinal) {
		Object value = this.fields[ordinal];
		if (value instanceof BinaryString) {
			return value.toString();
		} else {
			return (String) value;
		}
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		Object value = this.fields[ordinal];
		if (value instanceof Decimal) {
			return (Decimal) value;
		} else {
			return Decimal.fromBigDecimal((BigDecimal) value, precision, scale);
		}
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		return (T) this.fields[pos];
	}

	@Override
	public <T> T getGeneric(int pos, GenericType<T> type) {
		return (T) this.fields[pos];
	}

	@Override
	public BaseRow getBaseRow(int ordinal, int numFields) {
		return (BaseRow) this.fields[ordinal];
	}

	@Override
	public BaseArray getBaseArray(int ordinal) {
		return (BaseArray) this.fields[ordinal];
	}

	@Override
	public BaseMap getBaseMap(int ordinal) {
		return (BaseMap) this.fields[ordinal];
	}

	@Override
	public void setNullAt(int ordinal) {
		this.fields[ordinal] = null;
	}

	@Override
	public void setDecimal(int ordinal, Decimal value, int precision, int scale) {
		this.fields[ordinal] = value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getHeader()).append("|");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return 31 * Byte.hashCode(getHeader()) + Arrays.hashCode(fields);
	}

	@Override
	public boolean equals(Object o) {
		if (o != null && o instanceof ObjectArrayRow) {
			ObjectArrayRow other = (ObjectArrayRow) o;
			return header == other.header && Arrays.equals(fields, other.fields);
		} else {
			return false;
		}
	}

	private static boolean equalNonNullField(Object o1, Object o2) {
		if (o1 instanceof String && o2 instanceof BinaryString) {
			return o2.toString().equals(o1);
		} else if (o2 instanceof String && o1 instanceof BinaryString) {
			return o1.toString().equals(o2);
		} else {
			return o1.equals(o2);
		}
	}

	static boolean equalObjectArray(Object[] arr1, Object[] arr2) {

		int length = arr1.length;
		if (arr2.length != length) {
			return false;
		}

		for (int i = 0; i < length; i++) {
			Object o1 = arr1[i];
			Object o2 = arr2[i];
			if (!(o1 == null ? o2 == null : equalNonNullField(o1, o2))) {
				return false;
			}
		}
		return true;
	}
}
