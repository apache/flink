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

package org.apache.flink.orc.vector;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/**
 * This column vector is used to adapt hive's LongColumnVector to
 * Flink's boolean, byte, short, int and long ColumnVector.
 */
public class OrcLongColumnVector extends AbstractOrcColumnVector implements
		org.apache.flink.table.dataformat.vector.LongColumnVector,
		org.apache.flink.table.dataformat.vector.BooleanColumnVector,
		org.apache.flink.table.dataformat.vector.ByteColumnVector,
		org.apache.flink.table.dataformat.vector.ShortColumnVector,
		org.apache.flink.table.dataformat.vector.IntColumnVector {

	private LongColumnVector vector;

	public OrcLongColumnVector(LongColumnVector vector) {
		super(vector);
		this.vector = vector;
	}

	@Override
	public long getLong(int i) {
		return vector.vector[vector.isRepeating ? 0 : i];
	}

	@Override
	public boolean getBoolean(int i) {
		return vector.vector[vector.isRepeating ? 0 : i] == 1;
	}

	@Override
	public byte getByte(int i) {
		return (byte) vector.vector[vector.isRepeating ? 0 : i];
	}

	@Override
	public int getInt(int i) {
		return (int) vector.vector[vector.isRepeating ? 0 : i];
	}

	@Override
	public short getShort(int i) {
		return (short) vector.vector[vector.isRepeating ? 0 : i];
	}
}
