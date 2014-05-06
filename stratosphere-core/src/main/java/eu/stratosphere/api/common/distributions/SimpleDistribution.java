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
package eu.stratosphere.api.common.distributions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.types.Key;
import eu.stratosphere.util.InstantiationUtil;

public class SimpleDistribution implements DataDistribution {
	
	private static final long serialVersionUID = 1L;

	protected Key<?>[][] boundaries; 
	
	protected int dim;
	
	
	public SimpleDistribution() {
		boundaries = new Key[0][];
	}
	
	public SimpleDistribution(Key<?>[] bucketBoundaries) {
		if (bucketBoundaries == null) {
			throw new IllegalArgumentException("Bucket boundaries must not be null.");
		}
		if (bucketBoundaries.length == 0) {
			throw new IllegalArgumentException("Bucket boundaries must not be empty.");
		}

		// dimensionality is one in this case
		dim = 1;
		
		@SuppressWarnings("unchecked")
		Class<? extends Key<?>> clazz = (Class<? extends Key<?>>) bucketBoundaries[0].getClass();
		
		// make the array 2-dimensional
		boundaries = new Key[bucketBoundaries.length][];
		for (int i = 0; i < bucketBoundaries.length; i++) {
			if (bucketBoundaries[i].getClass() != clazz) {
				throw new IllegalArgumentException("The bucket boundaries are of different class types.");
			}
			
			boundaries[i] = new Key[] { bucketBoundaries[i] };
		}
	}
	
	@SuppressWarnings("unchecked")
	public SimpleDistribution(Key<?>[][] bucketBoundaries) {
		if (bucketBoundaries == null) {
			throw new IllegalArgumentException("Bucket boundaries must not be null.");
		}
		if (bucketBoundaries.length == 0) {
			throw new IllegalArgumentException("Bucket boundaries must not be empty.");
		}

		// dimensionality is one in this case
		dim = bucketBoundaries[0].length;
		
		Class<? extends Key<?>>[] types = new Class[dim];
		for (int i = 0; i < dim; i++) {
			types[i] = (Class<? extends Key<?>>) bucketBoundaries[0][i].getClass();
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
	public Key<?>[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		// check validity of arguments
		if(bucketNum < 0) {
			throw new IllegalArgumentException("Requested bucket must be greater than or equal to 0.");
		} else if(bucketNum >= (totalNumBuckets - 1)) {
			throw new IllegalArgumentException("Request bucket must be smaller than the total number of buckets minus 1.");
		}
		if(totalNumBuckets < 1) {
			throw new IllegalArgumentException("Total number of bucket must be larger than 0.");
		}
		
		final int maxNumBuckets = boundaries.length + 1;
		
		// check if max number of buckets is equal to or an even multiple of the requested number of buckets
		if((maxNumBuckets % totalNumBuckets) == 0) {
			// easy case, just use each n-th boundary
			final int n = maxNumBuckets / totalNumBuckets;
			final int bucketId = bucketNum * n + (n -  1); 
			
			return boundaries[bucketId];
		} else {
			throw new IllegalArgumentException("Interpolation of bucket boundaries currently not supported. " +
					"Please use an even divider of the maximum possible buckets (here: "+maxNumBuckets+") as totalBuckets.");
			// TODO: might be relaxed if much more boundary records are available than requested
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
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
	public void read(DataInput in) throws IOException {
		this.dim = in.readInt();
		final int len = in.readInt();
		
		boundaries = new Key[len][];
		
		// read types
		Class<? extends Key<?>>[] types = new Class[dim];
		for (int i = 0; i < dim; i++) {
			String className = in.readUTF();
			try {
				types[i] = (Class<? extends Key<?>>) Class.forName(className, true, getClass().getClassLoader()).asSubclass(Key.class);
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not load type class '" + className + "'.");
			} catch (Throwable t) {
				throw new IOException("Error loading type class '" + className + "'.", t);
			}
		}
		
		for (int i = 0; i < len; i++) {
			Key<?>[] bucket = new Key[dim]; 
			for (int d = 0; d < dim; d++) {
				Key<?> val = InstantiationUtil.instantiate(types[d], Key.class);
				val.read(in);
				bucket[d] = val;
			}
			
			boundaries[i] = bucket;
		}
	}
}
