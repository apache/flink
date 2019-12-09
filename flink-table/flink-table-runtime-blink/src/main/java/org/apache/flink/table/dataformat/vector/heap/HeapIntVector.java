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

import org.apache.flink.table.dataformat.vector.IntColumnVector;

/**
 * This class represents a nullable int column vector.
 */
public class HeapIntVector extends AbstractHeapVector implements IntColumnVector {

	private static final long serialVersionUID = -2749499358889718254L;

	public int[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapIntVector(int len) {
		super(len);
		vector = new int[len];
	}

	@Override
	public int getInt(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return dictionary.decodeToInt(dictionaryIds.vector[i]);
		}
	}
}
