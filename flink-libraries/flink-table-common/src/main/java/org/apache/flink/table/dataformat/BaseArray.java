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

package org.apache.flink.table.dataformat;

import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.Types;

/**
 * An interface for array used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryArray} format.
 * Convenient updates use the {@link GenericArray} format.
 */
public abstract class BaseArray implements TypeGetterSetters {

	public abstract int numElements();

	public abstract boolean isNullAt(int pos);

	public abstract void setNullAt(int pos);

	public abstract void setNotNullAt(int pos);

	public abstract void setNullLong(int pos);

	public abstract void setNullInt(int pos);

	public abstract void setNullBoolean(int pos);

	public abstract void setNullByte(int pos);

	public abstract void setNullShort(int pos);

	public abstract void setNullFloat(int pos);

	public abstract void setNullDouble(int pos);

	public abstract void setNullChar(int pos);

	public abstract boolean[] toBooleanArray();

	public abstract byte[] toByteArray();

	public abstract short[] toShortArray();

	public abstract int[] toIntArray();

	public abstract long[] toLongArray();

	public abstract float[] toFloatArray();

	public abstract double[] toDoubleArray();

	public Object toPrimitiveArray(InternalType type) {
		if (type.equals(Types.BOOLEAN)) {
			return toBooleanArray();
		} else if (type.equals(Types.BYTE)) {
			return toByteArray();
		} else if (type.equals(Types.SHORT)) {
			return toShortArray();
		} else if (type.equals(Types.INT)) {
			return toIntArray();
		} else if (type.equals(Types.LONG)) {
			return toLongArray();
		} else if (type.equals(Types.FLOAT)) {
			return toFloatArray();
		} else if (type.equals(Types.DOUBLE)) {
			return toDoubleArray();
		} else {
			throw new RuntimeException("Not a primitive type");
		}
	}

	public abstract Object[] toObjectArray(InternalType elementType);

	public abstract <T> T[] toClassArray(InternalType elementType, Class<T> clazz);

	public void setPrimitive(int pos, Object value, InternalType type) {
		if (type.equals(Types.BOOLEAN)) {
			setBoolean(pos, (boolean) value);
		} else if (type.equals(Types.BYTE)) {
			setByte(pos, (byte) value);
		} else if (type.equals(Types.SHORT)) {
			setShort(pos, (short) value);
		} else if (type.equals(Types.INT)) {
			setInt(pos, (int) value);
		} else if (type.equals(Types.LONG)) {
			setLong(pos, (long) value);
		} else if (type.equals(Types.FLOAT)) {
			setFloat(pos, (float) value);
		} else if (type.equals(Types.DOUBLE)) {
			setDouble(pos, (double) value);
		} else {
			throw new RuntimeException("Not a primitive type");
		}
	}
}
