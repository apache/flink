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

import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.util.StringUtils;

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
	public void setNullAt(int ordinal) {
		this.fields[ordinal] = null;
	}

	@Override
	public void setDecimal(int i, Decimal value, int precision) {
		this.fields[i] = value;
	}

	@Override
	public BinaryString getString(int ordinal) {
		return (BinaryString) this.fields[ordinal];
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return (byte[]) this.fields[ordinal];
	}

	@Override
	public BaseArray getArray(int ordinal) {
		return (BaseArray) this.fields[ordinal];
	}

	@Override
	public BaseMap getMap(int ordinal) {
		return (BaseMap) this.fields[ordinal];
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return (Decimal) this.fields[ordinal];
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int ordinal) {
		return (BinaryGeneric) this.fields[ordinal];
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		return (BaseRow) this.fields[ordinal];
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		if (BaseRowUtil.isAccumulateMsg(this)) {
			sb.append("+");
		} else {
			sb.append("-");
		}
		sb.append("|");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		sb.append(")");
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

	public boolean equalsWithoutHeader(BaseRow o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof ObjectArrayRow)) {
			return false;
		}

		ObjectArrayRow row = (ObjectArrayRow) o;
		return Arrays.equals(fields, row.fields);
	}
}
