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

import org.apache.flink.table.dataformat.vector.writable.WritableBooleanVector;

import java.util.Arrays;

/**
 * This class represents a nullable heap boolean column vector.
 */
public class HeapBooleanVector extends AbstractHeapVector implements WritableBooleanVector {

	private static final long serialVersionUID = 4131239076731313596L;

	public boolean[] vector;

	public HeapBooleanVector(int len) {
		super(len);
		vector = new boolean[len];
	}

	@Override
	public HeapIntVector reserveDictionaryIds(int capacity) {
		throw new RuntimeException("HeapBooleanVector has no dictionary.");
	}

	@Override
	public HeapIntVector getDictionaryIds() {
		throw new RuntimeException("HeapBooleanVector has no dictionary.");
	}

	@Override
	public boolean getBoolean(int i) {
		return vector[i];
	}

	@Override
	public void setBoolean(int i, boolean value) {
		vector[i] = value;
	}

	@Override
	public void fill(boolean value) {
		Arrays.fill(vector, value);
	}
}
