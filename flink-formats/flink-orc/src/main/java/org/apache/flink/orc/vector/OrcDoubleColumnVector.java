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

import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

/**
 * This column vector is used to adapt hive's DoubleColumnVector to Flink's float and double ColumnVector.
 */
public class OrcDoubleColumnVector extends AbstractOrcColumnVector implements
		org.apache.flink.table.dataformat.vector.DoubleColumnVector,
		org.apache.flink.table.dataformat.vector.FloatColumnVector {

	private DoubleColumnVector vector;

	public OrcDoubleColumnVector(DoubleColumnVector vector) {
		super(vector);
		this.vector = vector;
	}

	@Override
	public double getDouble(int i) {
		return vector.vector[vector.isRepeating ? 0 : i];
	}

	@Override
	public float getFloat(int i) {
		return (float) vector.vector[vector.isRepeating ? 0 : i];
	}
}
