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

import org.apache.flink.table.dataformat.vector.LongColumnVector;

/**
 * This class represents a nullable long column vector.
 */
public class HeapLongVector extends AbstractHeapVector implements LongColumnVector {

	private static final long serialVersionUID = 8534925169458006397L;

	public long[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapLongVector(int len) {
		super(len);
		vector = new long[len];
	}

	@Override
	public long getLong(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return dictionary.decodeToLong(dictionaryIds.vector[i]);
		}
	}
}
