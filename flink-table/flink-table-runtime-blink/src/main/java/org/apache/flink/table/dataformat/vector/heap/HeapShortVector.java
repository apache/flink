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

package org.apache.flink.table.dataformat.vector.heap;

import org.apache.flink.table.dataformat.vector.writable.WritableShortVector;

import java.util.Arrays;

/**
 * This class represents a nullable short column vector.
 */
public class HeapShortVector extends AbstractHeapVector implements WritableShortVector {

	private static final long serialVersionUID = -8278486456144676292L;

	public short[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapShortVector(int len) {
		super(len);
		vector = new short[len];
	}

	@Override
	public short getShort(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return (short) dictionary.decodeToInt(dictionaryIds.vector[i]);
		}
	}

	@Override
	public void setShort(int i, short value) {
		vector[i] = value;
	}

	@Override
	public void fill(short value) {
		Arrays.fill(vector, value);
	}
}
