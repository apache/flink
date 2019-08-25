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

import org.apache.flink.table.dataformat.vector.ByteColumnVector;

/**
 * This class represents a nullable byte column vector.
 */
public class HeapByteVector extends AbstractHeapVector implements ByteColumnVector {

	private static final long serialVersionUID = 7216045902943789034L;

	public byte[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapByteVector(int len) {
		super(len);
		vector = new byte[len];
	}

	@Override
	public byte getByte(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return (byte) dictionary.decodeToInt(dictionaryIds.vector[i]);
		}
	}
}
