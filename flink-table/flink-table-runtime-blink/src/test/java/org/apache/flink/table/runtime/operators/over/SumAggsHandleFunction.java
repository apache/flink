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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;

/**
 * Test {@link AggsHandleFunction}.
 */
public class SumAggsHandleFunction implements AggsHandleFunction {

	private final int inputIndex;
	private long sum;

	public SumAggsHandleFunction(int inputIndex) {
		this.inputIndex = inputIndex;
	}

	@Override
	public void open(StateDataViewStore store) throws Exception {
	}

	@Override
	public void accumulate(BaseRow input) throws Exception {
		sum += input.getLong(inputIndex);
	}

	@Override
	public void retract(BaseRow input) throws Exception {
		sum -= input.getLong(inputIndex);
	}

	@Override
	public void merge(BaseRow accumulator) throws Exception {
		sum += accumulator.getLong(0);
	}

	@Override
	public void setAccumulators(BaseRow accumulator) throws Exception {
		sum = accumulator.getLong(0);
	}

	@Override
	public void resetAccumulators() throws Exception {
		sum  = 0L;
	}

	@Override
	public BaseRow getAccumulators() throws Exception {
		return GenericRow.of(sum);
	}

	@Override
	public BaseRow createAccumulators() throws Exception {
		return GenericRow.of(0L);
	}

	@Override
	public BaseRow getValue() throws Exception {
		return getAccumulators();
	}

	@Override
	public void cleanup() throws Exception {
	}

	@Override
	public void close() throws Exception {
	}
}
