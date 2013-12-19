/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleCounter implements SimpleAccumulator<Double> {

	private static final long serialVersionUID = 1L;

	private double localValue = 0;

	@Override
	public void add(Double value) {
		localValue += value;
	}

	@Override
	public Double getLocalValue() {
		return localValue;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		this.localValue += ((DoubleCounter) other).getLocalValue();
	}

	@Override
	public void resetLocal() {
		this.localValue = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(localValue);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.localValue = in.readDouble();
	}

	@Override
	public String toString() {
		return "DoubleCounter object. Local value: " + this.localValue;
	}

}
