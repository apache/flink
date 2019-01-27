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

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.dataformat.vector.BooleanColumnVector;
import org.apache.flink.table.dataformat.vector.ByteColumnVector;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.table.dataformat.vector.FloatColumnVector;
import org.apache.flink.table.dataformat.vector.IntegerColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;

import org.apache.parquet.io.api.Binary;

/**
 * Interface for value decoding that supports vectorized (aka batched) decoding.
 */
public interface VectorizedValuesReader {

	boolean readBoolean();

	byte readByte();

	int readInteger();

	long readLong();

	float readFloat();

	double readDouble();

	Binary readBinary(int len);

	/*
	 * Reads `total` values into `c` start at `c[rowId]`
	 */
	void readBooleans(int total, BooleanColumnVector c, int rowId);

	void readBytes(int total, ByteColumnVector c, int rowId);

	void readIntegers(int total, IntegerColumnVector c, int rowId);

	void readLongs(int total, LongColumnVector c, int rowId);

	void readFloats(int total, FloatColumnVector c, int rowId);

	void readDoubles(int total, DoubleColumnVector c, int rowId);

	void readBinaries(int total, BytesColumnVector c, int rowId);
}
