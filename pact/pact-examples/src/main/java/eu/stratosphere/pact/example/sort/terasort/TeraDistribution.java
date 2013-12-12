/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.sort.terasort;

import java.io.DataInput;
import java.io.DataOutput;

import eu.stratosphere.pact.common.distributions.DataDistribution;
import eu.stratosphere.pact.common.type.Key;

/**
 * This class implements the uniform data distribution of the TeraSort benchmark.
 */
public class TeraDistribution implements DataDistribution  {

	private static final long serialVersionUID = 1L;
	private static final int ALPHABETH_SIZE = 95;

	@Override
	public Key[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		final byte[] buf = new byte[TeraKey.KEY_SIZE];
		double threshold = (double) ALPHABETH_SIZE / (double) (totalNumBuckets + 1) * (double) (bucketNum + 1);

		for (int i = 0; i < buf.length; ++i) {
			final int ch = (int) Math.floor(threshold) % ALPHABETH_SIZE;
			buf[i] = (byte) (' ' + ch);

			threshold = threshold - (double) ch;
			threshold = threshold * ALPHABETH_SIZE;
		}

		return new TeraKey[] { new TeraKey(buf, 0) };
	}
	
	@Override
	public int getNumberOfFields() {
		return 1;
	}

	@Override
	public void write(DataOutput out) {}

	@Override
	public void read(DataInput in) {}
}
