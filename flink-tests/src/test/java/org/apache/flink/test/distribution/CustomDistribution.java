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

package org.apache.flink.test.distribution;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * The class is used to do the tests of range partition with customed data distribution.
 */
public class CustomDistribution implements DataDistribution {

	protected Value[][] boundaries;

	protected int dim;
	
	public CustomDistribution() {}
	
	public CustomDistribution(Value[] bucketBoundaries) {
		
		if (bucketBoundaries == null) {
			throw new IllegalArgumentException("source key cannot be null");
		}
		
		dim = 1;

		@SuppressWarnings("unchecked")
		Class<? extends Value> clazz = bucketBoundaries[0].getClass();
		
		boundaries = new Value[bucketBoundaries.length][];
		for (int i = 0; i < bucketBoundaries.length; i++) {
			if (bucketBoundaries[i].getClass() != clazz) {
				throw new IllegalArgumentException("The bucket boundaries are of different class types.");
			}
			boundaries[i] = new Value[] { bucketBoundaries[i] };
		}
	}

	public CustomDistribution(Value[][] bucketBoundaries) {
		if (bucketBoundaries == null) {
			throw new IllegalArgumentException("Bucket boundaries must not be null.");
		}
		if (bucketBoundaries.length == 0) {
			throw new IllegalArgumentException("Bucket boundaries must not be empty.");
		}

		// dimensionality is one in this case
		dim = bucketBoundaries[0].length;

		Class<? extends Value>[] types = new Class[dim];
		for (int i = 0; i < dim; i++) {
			types[i] = bucketBoundaries[0][i].getClass();
		}

		// check the array
		for (int i = 1; i < bucketBoundaries.length; i++) {
			if (bucketBoundaries[i].length != dim) {
				throw new IllegalArgumentException("All bucket boundaries must have the same dimensionality.");
			}
			for (int d = 0; d < dim; d++) {
				if (types[d] != bucketBoundaries[i][d].getClass()) {
					throw new IllegalArgumentException("The bucket boundaries are of different class types.");
				}
			}
		}

		boundaries = bucketBoundaries;
	}

	@Override
	public int getNumberOfFields() {
		return this.dim;
	}
	
	@Override
	public Value[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		// check validity of arguments
		if(bucketNum < 0) {
			throw new IllegalArgumentException("Requested bucket must be greater than or equal to 0.");
		} else if(bucketNum >= (totalNumBuckets)) {
			throw new IllegalArgumentException("Request bucket must be smaller than the total number of buckets.");
		}
		if(totalNumBuckets < 1) {
			throw new IllegalArgumentException("Total number of bucket must be larger than 0.");
		}

		final int maxNumBuckets = boundaries.length + 1;
		final int n = maxNumBuckets / totalNumBuckets;
		final int bucketId = bucketNum * n + (n - 1);

		return boundaries[bucketId];
		
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(this.dim);
		out.writeInt(boundaries.length);

		// write types
		for (int i = 0; i < dim; i++) {
			out.writeUTF(boundaries[0][i].getClass().getName());
		}

		for (int i = 0; i < boundaries.length; i++) {
			for (int d = 0; d < dim; d++) {
				boundaries[i][d].write(out);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(DataInputView in) throws IOException {
		this.dim = in.readInt();
		final int len = in.readInt();

		boundaries = new Value[len][];

		// read types
		Class<? extends Value>[] types = new Class[dim];
		for (int i = 0; i < dim; i++) {
			String className = in.readUTF();
			try {
				types[i] = Class.forName(className, true, getClass().getClassLoader()).asSubclass(Value.class);
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not load type class '" + className + "'.");
			} catch (Throwable t) {
				throw new IOException("Error loading type class '" + className + "'.", t);
			}
		}

		for (int i = 0; i < len; i++) {
			Value[] bucket = new Value[dim];
			for (int d = 0; d < dim; d++) {
				Value val = InstantiationUtil.instantiate(types[d], Value.class);
				val.read(in);
				bucket[d] = val;
			}
			boundaries[i] = bucket;
		}
	}
}
