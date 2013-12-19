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
package eu.stratosphere.api.distributions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.IntValue;


public class UniformIntegerDistribution implements DataDistribution {

	private static final long serialVersionUID = 1L;
	
	private int min, max; 

	
	public UniformIntegerDistribution() {}
	
	public UniformIntegerDistribution(int min, int max) {
		this.min = min;
		this.max = max;
	}

	@Override
	public IntValue[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		long diff = ((long) max) - ((long) min) + 1;
		double bucketSize = diff / ((double) totalNumBuckets);
		return new IntValue[] {new IntValue(min + (int) ((bucketNum+1) * bucketSize)) };
	}

	@Override
	public int getNumberOfFields() {
		return 1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(min);
		out.writeInt(max);
	}

	@Override
	public void read(DataInput in) throws IOException {
		min = in.readInt();
		max = in.readInt();
	}
}
