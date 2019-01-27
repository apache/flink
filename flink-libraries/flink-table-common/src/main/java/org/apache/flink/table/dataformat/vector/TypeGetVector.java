/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.vector;

import org.apache.flink.table.dataformat.Decimal;

/**
 * ColumnVector to support getInt getDouble...
 */
public abstract class TypeGetVector extends ColumnVector {

	public TypeGetVector(int len) {
		super(len);
	}

	public abstract boolean getBoolean(int rowId);

	public abstract byte getByte(int rowId);

	public abstract short getShort(int rowId);

	public abstract int getInt(int rowId);

	public abstract long getLong(int rowId);

	public abstract float getFloat(int rowId);

	public abstract double getDouble(int rowId);

	public abstract VectorizedColumnBatch.ByteArray getByteArray(int rowId);

	public abstract Decimal getDecimal(int rowId, int precision, int scala);
}
