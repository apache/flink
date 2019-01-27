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

package org.apache.flink.table.sources.orc;

import org.apache.flink.table.dataformat.vector.ColumnVector;

import java.util.Arrays;

/**
 * This class represents a Missing ColumnVector who's noNulls is false and all fields are null.
 * It's a placehold for some vectorized reader {@link OrcVectorizedReader}
 */
public class MissingColumnVector extends ColumnVector {

	private static final long serialVersionUID = -4386528684821571126L;

	public MissingColumnVector(int len) {
		super(len);
		Arrays.fill(isNull, true);
		noNulls = false;
	}

	@Override
	public Object get(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
		throw new UnsupportedOperationException();
	}
}
