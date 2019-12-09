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

/**
 * An interface for array used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryArray} format.
 * Convenient updates use the {@link GenericArray} format.
 */
public interface BaseArray extends TypeGetterSetters {

	int numElements();

	boolean isNullAt(int pos);

	void setNullAt(int pos);

	void setNotNullAt(int pos);

	void setNullLong(int pos);

	void setNullInt(int pos);

	void setNullBoolean(int pos);

	void setNullByte(int pos);

	void setNullShort(int pos);

	void setNullFloat(int pos);

	void setNullDouble(int pos);

	boolean[] toBooleanArray();

	byte[] toByteArray();

	short[] toShortArray();

	int[] toIntArray();

	long[] toLongArray();

	float[] toFloatArray();

	double[] toDoubleArray();
}
