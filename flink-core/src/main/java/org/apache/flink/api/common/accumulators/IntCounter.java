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


package org.apache.flink.api.common.accumulators;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public class IntCounter implements SimpleAccumulator<Integer> {

	private static final long serialVersionUID = 1L;

	private int localValue = 0;

	@Override
	public void add(Integer value) {
		localValue += value;
	}

	@Override
	public Integer getLocalValue() {
		return localValue;
	}

	@Override
	public void merge(Accumulator<Integer, Integer> other) {
		this.localValue += ((IntCounter) other).getLocalValue();
	}

	@Override
	public void resetLocal() {
		this.localValue = 0;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(localValue);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		localValue = in.readInt();
	}

	@Override
	public String toString() {
		return "IntCounter object. Local value: " + this.localValue;
	}

}
